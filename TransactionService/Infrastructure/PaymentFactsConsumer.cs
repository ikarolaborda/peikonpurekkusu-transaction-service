using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Peikon.Transactions.Domain;

namespace Peikon.Transactions.Infrastructure;

/// <summary>
/// Consumes payment facts and records immutable transactions:
///   payments.payment.captured.v1 → purchase row
///   payments.payment.reversed.v1 → parked to DLQ (contract lacks refund fields)
/// Payloads are validated against the schema id in the frame before any field
/// is read — the transactions table is append-only, so a drifted producer must
/// be dead-lettered, never written as a zero-amount row. Idempotent
/// (processed_events in the same DbContext transaction), offsets stored only
/// after durable writes, 3 attempts → per-group DLQ.
/// </summary>
public sealed class PaymentFactsConsumer(IServiceScopeFactory scopes, IProducer<string, byte[]> producer,
    EventContractValidator validator, IConfiguration config, ILogger<PaymentFactsConsumer> log) : BackgroundService
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
            ConsumeResult<string, byte[]>? result = null;
            try
            {
                result = consumer.Consume(ct);
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
            catch (SchemaRegistryUnavailableException ex)
            {
                // Not poison and not skippable: if we moved on, the NEXT record's
                // StoreOffset would commit past this one and the unvalidated
                // event would be lost. Seek back and block until the registry
                // answers — liveness traded for never writing an unvalidated fact.
                log.LogWarning(ex, "schema registry unavailable — holding {Offset}", result!.TopicPartitionOffset);
                consumer.Seek(result.TopicPartitionOffset);
                Thread.Sleep(2000);
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
            catch (PoisonEventException ex)
            {
                // Retrying cannot fix it, and the DLQ result decides the offset:
                // discarding it here would drop the message when the DLQ write fails.
                return await DeadLetterAsync(result, ex.InnerException ?? ex, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException and not SchemaRegistryUnavailableException)
            {
                last = ex;
                log.LogWarning(ex, "record failed (attempt {Attempt})", attempt);
                await Task.Delay(attempt * 200, ct);
            }
        }
        // Only advance the offset if the message is safely parked in the DLQ.
        // If the DLQ write fails (e.g. coordinator flap), return false so the
        // offset is NOT stored and the message is reprocessed — never lost.
        return await DeadLetterAsync(result, last, ct);
    }

    private async Task HandleAsync(ConsumeResult<string, byte[]> result, CancellationToken ct)
    {
        var parsed = EventsCodec.TryParseFrame(result.Message.Value);
        if (parsed is null)
        {
            throw new PoisonEventException(new FormatException("unparseable envelope"));
        }

        try
        {
            await validator.ValidateAsync(parsed.Value.SchemaId, parsed.Value.Node, ct);
        }
        catch (Exception ex) when (ex is EventContractViolationException or UnknownSchemaIdException)
        {
            throw new PoisonEventException(ex);
        }
        // SchemaRegistryUnavailableException deliberately bubbles: transient,
        // the run loop seeks back so the offset can never advance past it.

        var envelope = EventsCodec.ToEnvelope(parsed.Value.Node);
        if (envelope is null || !Guid.TryParse(envelope.EventId, out var eventId))
        {
            throw new PoisonEventException(new FormatException("unparseable envelope"));
        }

        if (envelope.EventType.Contains("reversed"))
        {
            // The reversed.v1 payload cannot carry account/user/merchant fields
            // (additionalProperties:false), so a refund fact recorded from it
            // would be a permanent garbage row. Park until the refunds contract
            // is completed; the DLQ keeps it replayable.
            throw new PoisonEventException(new NotSupportedException(
                "payments.payment.reversed.v1 lacks the fields a refund fact needs — parked until the refunds contract is built"));
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
        catch (DbUpdateException ex) when (ex.IsUniqueViolation())
        {
            return; // genuinely already processed
        }
        // Any other DbUpdateException (deadlock, timeout, broken connection) is a
        // real failure: let it bubble so the event is retried, not silently acked.

        var p = envelope.Payload;
        var command = new RecordTransaction(
            eventId,
            Require(p, "payment_id"),
            Require(p, "account_id"),
            Require(p, "user_id"),
            (string?)p["merchant_id"] ?? "",
            "purchase",
            (long?)p["amount_minor_units"]
                ?? throw new PoisonEventException(new FormatException("payload missing 'amount_minor_units'")),
            Require(p, "currency_code"),
            Require(p, "ledger_transaction_id"),
            (string?)p["psp_reference"] ?? "",
            envelope.OccurredAt);
        await mediator.SendAsync(command, ct);

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }

    /// Validation guarantees these fields; the throw is the backstop that keeps
    /// any future bypass from synthesizing money values out of nothing.
    private static string Require(System.Text.Json.Nodes.JsonObject p, string key) =>
        (string?)p[key]
            ?? throw new PoisonEventException(new FormatException($"payload missing '{key}'"));

    /// <returns>true if the message is safely in the DLQ (offset may advance); false if the DLQ write failed (retry, don't lose it).</returns>
    private async Task<bool> DeadLetterAsync(ConsumeResult<string, byte[]> result, Exception? cause, CancellationToken ct)
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
            return true;
        }
        catch (Exception ex)
        {
            log.LogError(ex, "DLQ publish failed — leaving offset unadvanced for retry ({Cause})", cause?.Message);
            return false;
        }
    }
}
