using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Peikon.Transactions.Domain;

namespace Peikon.Transactions.Infrastructure;

/// <summary>
/// Consumes payment facts and records immutable transactions:
///   payments.payment.captured.v1 → purchase row
///   payments.payment.reversed.v1 → refund (reversing) row
/// Idempotent (processed_events in the same DbContext transaction), offsets
/// stored only after durable writes, 3 attempts → per-group DLQ.
/// </summary>
public sealed class PaymentFactsConsumer(IServiceScopeFactory scopes, IProducer<string, byte[]> producer,
    IConfiguration config, ILogger<PaymentFactsConsumer> log) : BackgroundService
{
    private const string Group = "transaction-service";
    private static readonly string[] Topics =
        ["payments.payment.captured.v1", "payments.payment.reversed.v1"];

    protected override Task ExecuteAsync(CancellationToken ct) =>
        Task.Factory.StartNew(() => RunLoop(ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private void RunLoop(CancellationToken ct)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["KAFKA_BOOTSTRAP_SERVERS"] ?? "kafka:19092",
            GroupId = Group,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
        };
        using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(Topics);
        log.LogInformation("consumer subscribed to {Topics}", string.Join(",", Topics));

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(ct);
                if (result is null) continue;
                if (HandleWithRetryAsync(result, ct).GetAwaiter().GetResult())
                {
                    consumer.StoreOffset(result);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                log.LogError(ex, "consume error");
                Thread.Sleep(1000);
            }
        }
        consumer.Close();
    }

    private async Task<bool> HandleWithRetryAsync(ConsumeResult<string, byte[]> result, CancellationToken ct)
    {
        Exception? last = null;
        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                await HandleAsync(result, ct);
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                last = ex;
                log.LogWarning(ex, "record failed (attempt {Attempt})", attempt);
                await Task.Delay(attempt * 200, ct);
            }
        }
        await DeadLetterAsync(result, last, ct);
        return true;
    }

    private async Task HandleAsync(ConsumeResult<string, byte[]> result, CancellationToken ct)
    {
        var envelope = EventsCodec.TryUnframe(result.Message.Value);
        if (envelope is null || !Guid.TryParse(envelope.EventId, out var eventId))
        {
            await DeadLetterAsync(result, new FormatException("unparseable envelope"), ct);
            return;
        }

        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TxDbContext>();
        var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        db.ProcessedEvents.Add(new ProcessedEvent { EventId = eventId });
        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateException)
        {
            return; // already processed
        }

        var p = envelope.Payload;
        var command = new RecordTransaction(
            eventId,
            (string?)p["payment_id"] ?? "",
            (string?)p["account_id"] ?? "",
            (string?)p["user_id"] ?? "",
            (string?)p["merchant_id"] ?? "",
            envelope.EventType.Contains("reversed") ? "refund" : "purchase",
            (long?)p["amount_minor_units"] ?? 0,
            (string?)p["currency_code"] ?? "",
            (string?)p["ledger_transaction_id"] ?? (string?)p["reversal_ledger_transaction_id"] ?? "",
            (string?)p["psp_reference"] ?? "",
            envelope.OccurredAt);
        await mediator.SendAsync(command, ct);

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }

    private async Task DeadLetterAsync(ConsumeResult<string, byte[]> result, Exception? cause, CancellationToken ct)
    {
        var dlq = $"{Group}.{result.Topic}.dlq";
        var message = new Message<string, byte[]>
        {
            Key = result.Message.Key,
            Value = result.Message.Value,
            Headers = new Headers
            {
                { "x-exception", System.Text.Encoding.UTF8.GetBytes(cause?.Message ?? "unknown") },
                { "x-original-topic", System.Text.Encoding.UTF8.GetBytes(result.Topic) },
                { "x-original-partition", System.Text.Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                { "x-original-offset", System.Text.Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                { "x-failed-at", System.Text.Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O")) },
                { "x-consumer-group", System.Text.Encoding.UTF8.GetBytes(Group) },
            },
        };
        try
        {
            await producer.ProduceAsync(dlq, message, ct);
            log.LogWarning("message dead-lettered to {Dlq}: {Cause}", dlq, cause?.Message);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "DLQ publish FAILED — message dropped ({Cause})", cause?.Message);
        }
    }
}
