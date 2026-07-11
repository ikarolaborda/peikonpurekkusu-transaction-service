using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Peikon.Transactions.Domain;
using Peikon.Transactions.Infrastructure;

// `TransactionService healthcheck` — self-probe for the chiseled image.
if (args.Length > 0 && args[0] == "healthcheck")
{
    using var probe = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    try
    {
        var res = await probe.GetAsync("http://localhost:8080/health/ready");
        return res.IsSuccessStatusCode ? 0 : 1;
    }
    catch
    {
        return 1;
    }
}

var builder = WebApplication.CreateBuilder(args);
string Env(string key, string fallback) => builder.Configuration[key] ?? fallback;

var dbConn = $"Host={Env("TRANSACTION_DB_HOST", "transaction-db")};Port={Env("TRANSACTION_DB_PORT", "5432")};" +
             $"Username={Env("TRANSACTION_DB_USER", "")};Password={Env("TRANSACTION_DB_PASSWORD", "")};" +
             $"Database={Env("TRANSACTION_DB_NAME", "")}";

builder.Services.AddDbContext<TxDbContext>(o => o.UseNpgsql(dbConn));
builder.Services.AddScoped<IMediator, Mediator>();
builder.Services.AddScoped<ICommandHandler<RecordTransaction, Guid>, RecordTransactionHandler>();
builder.Services.AddValidation();

builder.Services.AddSingleton<IProducer<string, byte[]>>(_ =>
    new ProducerBuilder<string, byte[]>(new ProducerConfig
    {
        BootstrapServers = Env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
        EnableIdempotence = true,
        Acks = Acks.All,
    }).Build());
builder.Services.AddSingleton(_ => new EventsCodec(
    new HttpClient(),
    Env("SCHEMA_REGISTRY_URL", "http://apicurio-registry:8080/apis/ccompat/v7")));
builder.Services.AddSingleton(sp => new EventContractValidator(
    new HttpClient { Timeout = TimeSpan.FromSeconds(5) },
    Env("SCHEMA_REGISTRY_URL", "http://apicurio-registry:8080/apis/ccompat/v7"),
    sp.GetRequiredService<ILogger<EventContractValidator>>()));

builder.Services.AddHostedService<OutboxRelay>();
builder.Services.AddHostedService<PaymentFactsConsumer>();
builder.Services.AddHealthChecks().AddNpgSql(dbConn, tags: ["ready"]);

builder.Services.AddSingleton(sp => new GatewayAssertionValidator(
    new HttpClient { Timeout = TimeSpan.FromSeconds(5) },
    Env("GATEWAY_JWKS_URL", "http://user-service:8080/.well-known/jwks.json"),
    sp.GetRequiredService<ILogger<GatewayAssertionValidator>>()));

var app = builder.Build();

await app.Services.GetRequiredService<GatewayAssertionValidator>().InitializeAsync();

// Trust identity only from a verified gateway assertion, never from a raw header a
// peer on the internal network could forge. Runs before the endpoints, which keep
// reading X-User-Id — now guaranteed to originate from ForwardAuth. Health is open.
app.Use(async (http, next) =>
{
    if (http.Request.Path.StartsWithSegments("/health"))
    {
        await next();
        return;
    }

    var validator = http.RequestServices.GetRequiredService<GatewayAssertionValidator>();
    var assertion = http.Request.Headers["X-Gateway-Assertion"].ToString();
    var userId = string.IsNullOrEmpty(assertion) ? null : await validator.ValidateAsync(assertion);
    if (string.IsNullOrEmpty(userId))
    {
        http.Response.StatusCode = StatusCodes.Status401Unauthorized;
        await http.Response.WriteAsJsonAsync(new { title = "Unauthorized", status = 401, detail = "invalid gateway assertion" });
        return;
    }

    http.Request.Headers.Remove("X-User-Id");
    http.Request.Headers.Remove("X-Auth-Amr");
    http.Request.Headers.Remove("X-Auth-Time");
    http.Request.Headers.Remove("X-Gateway-Assertion");
    http.Request.Headers["X-User-Id"] = userId;
    await next();
});

await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
    await db.Database.EnsureCreatedAsync();
    await db.Database.ExecuteSqlRawAsync(TxDbContext.ImmutabilityTriggers);
}

app.MapGet("/health/live", () => Results.Ok(new { status = "ok" }));
app.MapHealthChecks("/health/ready", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = c => c.Tags.Contains("ready"),
});

var txns = app.MapGroup("/transactions");

// List (query side). Identity from the ForwardAuth header; cursor = recorded_at ISO.
txns.MapGet("/", async (HttpContext http, TxDbContext db, string? account_id, string? cursor, CancellationToken ct) =>
{
    var userId = http.Request.Headers["X-User-Id"].ToString();
    if (string.IsNullOrEmpty(userId)) return Results.Unauthorized();

    var q = db.Transactions.AsNoTracking().Where(t => t.UserId == userId);
    if (!string.IsNullOrEmpty(account_id)) q = q.Where(t => t.AccountId == account_id);
    if (DateTimeOffset.TryParse(cursor, out var before)) q = q.Where(t => t.RecordedAt < before);

    var rows = await q.OrderByDescending(t => t.RecordedAt).Take(50).ToListAsync(ct);
    var next = rows.Count == 50 ? rows[^1].RecordedAt.ToString("O") : null;
    return Results.Ok(new
    {
        transactions = rows.Select(t => new
        {
            transaction_id = t.Id,
            payment_id = t.PaymentId,
            account_id = t.AccountId,
            merchant_id = t.MerchantId,
            transaction_type = t.Type,
            amount_minor_units = t.AmountMinorUnits,
            currency_code = t.CurrencyCode,
            occurred_at = t.OccurredAt,
            recorded_at = t.RecordedAt,
        }),
        next_cursor = next,
    });
});

txns.MapGet("/{id:guid}", async (HttpContext http, TxDbContext db, Guid id, CancellationToken ct) =>
{
    var userId = http.Request.Headers["X-User-Id"].ToString();
    if (string.IsNullOrEmpty(userId)) return Results.Unauthorized();
    var t = await db.Transactions.AsNoTracking().FirstOrDefaultAsync(x => x.Id == id && x.UserId == userId, ct);
    return t is null
        ? Results.NotFound()
        : Results.Ok(new
        {
            transaction_id = t.Id,
            payment_id = t.PaymentId,
            account_id = t.AccountId,
            merchant_id = t.MerchantId,
            transaction_type = t.Type,
            amount_minor_units = t.AmountMinorUnits,
            currency_code = t.CurrencyCode,
            ledger_transaction_id = t.LedgerTransactionId,
            psp_reference = t.PspReference,
            occurred_at = t.OccurredAt,
            recorded_at = t.RecordedAt,
        });
});

app.Run();
return 0;
