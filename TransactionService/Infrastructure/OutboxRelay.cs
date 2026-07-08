using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;

namespace Peikon.Transactions.Infrastructure;

/// <summary>
/// Polling transactional-outbox relay (mirror of the Go services):
/// FOR UPDATE SKIP LOCKED batch → frame → produce (acks=all, idempotent) →
/// mark processed. At-least-once; consumers dedupe on event_id.
/// </summary>
public sealed class OutboxRelay(IServiceScopeFactory scopes, IProducer<string, byte[]> producer,
    EventsCodec codec, ILogger<OutboxRelay> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
        {
            try
            {
                await DrainOnceAsync(ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                log.LogError(ex, "outbox drain failed (will retry)");
            }
        }
    }

    internal async Task<int> DrainOnceAsync(CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var rows = await db.Outbox
            .FromSqlRaw("""
                select * from outbox where processed_at is null
                order by id limit 50 for update skip locked
                """)
            .AsTracking()
            .ToListAsync(ct);
        if (rows.Count == 0)
        {
            return 0;
        }

        foreach (var row in rows)
        {
            var envelope = new
            {
                event_id = row.Id.ToString(),
                event_type = row.Type,
                schema_version = 1,
                occurred_at = row.CreatedAt.UtcDateTime.ToString("O"),
                tenant_id = "peikon",
                correlation_id = row.Id.ToString("N")[..32],
                causation_id = (string?)null,
                idempotency_key = (string?)null,
                // parse so the stored JSON string embeds as an object, not a quoted literal
                payload = System.Text.Json.Nodes.JsonNode.Parse(row.Payload),
            };
            var value = await codec.FrameAsync(row.Type, envelope, ct);
            await producer.ProduceAsync(row.Type,
                new Message<string, byte[]> { Key = row.AggregateId, Value = value }, ct);
            row.ProcessedAt = DateTimeOffset.UtcNow;
        }
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return rows.Count;
    }
}
