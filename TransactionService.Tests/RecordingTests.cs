using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Peikon.Transactions.Domain;
using Peikon.Transactions.Infrastructure;
using Xunit;

namespace Peikon.Transactions.Tests;

public class RecordingTests
{
    private static TxDbContext NewDb() =>
        new(new DbContextOptionsBuilder<TxDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options);

    private static RecordTransaction Capture(string paymentId, string type = "purchase", long amount = 1250) =>
        new(Guid.CreateVersion7(), paymentId, "acct-1", "user-1", "m-coffee", type, amount, "USD",
            "ledger-1", "psp-1", DateTimeOffset.UtcNow);

    [Fact]
    public async Task Records_an_immutable_transaction_and_stages_the_recorded_event()
    {
        await using var db = NewDb();
        var handler = new RecordTransactionHandler(db);

        var id = await handler.HandleAsync(Capture("pay-1"), CancellationToken.None);
        await db.SaveChangesAsync();

        var txn = await db.Transactions.SingleAsync();
        Assert.Equal(id, txn.Id);
        Assert.Equal("purchase", txn.Type);
        Assert.Equal(1250, txn.AmountMinorUnits);

        var outbox = await db.Outbox.SingleAsync();
        Assert.Equal("transactions.transaction.recorded.v1", outbox.Type);
    }

    [Fact]
    public async Task Corrections_are_new_reversing_rows_never_updates()
    {
        await using var db = NewDb();
        var handler = new RecordTransactionHandler(db);

        await handler.HandleAsync(Capture("pay-2"), CancellationToken.None);
        await db.SaveChangesAsync();
        await handler.HandleAsync(Capture("pay-2", type: "refund"), CancellationToken.None);
        await db.SaveChangesAsync();

        var rows = await db.Transactions.Where(t => t.PaymentId == "pay-2").ToListAsync();
        Assert.Equal(2, rows.Count); // original + reversing row, both present
        Assert.Contains(rows, r => r.Type == "purchase");
        Assert.Contains(rows, r => r.Type == "refund");
    }

    [Fact]
    public async Task Mediator_dispatches_to_the_registered_handler()
    {
        await using var db = NewDb();
        var services = new ServiceCollection();
        services.AddScoped<TxDbContext>(_ => db);
        services.AddScoped<ICommandHandler<RecordTransaction, Guid>, RecordTransactionHandler>();
        var provider = services.BuildServiceProvider();
        var mediator = new Mediator(provider);

        var id = await mediator.SendAsync(Capture("pay-3"), CancellationToken.None);
        await db.SaveChangesAsync();

        Assert.NotEqual(Guid.Empty, id);
        Assert.Equal(1, await db.Transactions.CountAsync());
    }
}

public class EnvelopeTests
{
    [Fact]
    public void Unframe_rejects_raw_json_without_the_confluent_header()
    {
        Assert.Null(EventsCodec.TryUnframe(System.Text.Encoding.UTF8.GetBytes("{\"event_id\":\"x\"}")));
    }

    [Fact]
    public void Unframe_parses_a_framed_envelope()
    {
        var json = System.Text.Encoding.UTF8.GetBytes(
            "{\"event_id\":\"e1\",\"event_type\":\"payments.payment.captured.v1\",\"payload\":{\"payment_id\":\"p1\"}}");
        var framed = new byte[5 + json.Length];
        framed[0] = 0;
        json.CopyTo(framed, 5);
        var env = EventsCodec.TryUnframe(framed);
        Assert.NotNull(env);
        Assert.Equal("payments.payment.captured.v1", env!.EventType);
        Assert.Equal("p1", (string?)env.Payload["payment_id"]);
    }
}
