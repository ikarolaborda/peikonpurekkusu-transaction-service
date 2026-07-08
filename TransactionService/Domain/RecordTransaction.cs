using System.Text.Json;
using Peikon.Transactions.Infrastructure;

namespace Peikon.Transactions.Domain;

/// <summary>
/// Records one immutable transaction row from a payment fact. Corrections are
/// new reversing rows — this handler never updates. Idempotency belongs to
/// the caller (consumer inserts processed_events in the same DbContext tx).
/// </summary>
public sealed record RecordTransaction(
    Guid EventId,
    string PaymentId,
    string AccountId,
    string UserId,
    string MerchantId,
    string TransactionType, // purchase | refund | chargeback
    long AmountMinorUnits,
    string CurrencyCode,
    string LedgerTransactionId,
    string PspReference,
    DateTimeOffset OccurredAt) : ICommand<Guid>;

public sealed class RecordTransactionHandler(TxDbContext db) : ICommandHandler<RecordTransaction, Guid>
{
    public async Task<Guid> HandleAsync(RecordTransaction cmd, CancellationToken ct)
    {
        var txn = new Transaction
        {
            PaymentId = cmd.PaymentId,
            AccountId = cmd.AccountId,
            UserId = cmd.UserId,
            MerchantId = cmd.MerchantId,
            Type = cmd.TransactionType,
            AmountMinorUnits = cmd.AmountMinorUnits,
            CurrencyCode = cmd.CurrencyCode,
            LedgerTransactionId = cmd.LedgerTransactionId,
            PspReference = cmd.PspReference,
            OccurredAt = cmd.OccurredAt,
        };
        db.Transactions.Add(txn);

        db.Outbox.Add(new OutboxEvent
        {
            AggregateType = "transaction",
            AggregateId = txn.Id.ToString(),
            Type = "transactions.transaction.recorded.v1",
            Payload = JsonSerializer.Serialize(new
            {
                transaction_id = txn.Id.ToString(),
                payment_id = cmd.PaymentId,
                account_id = cmd.AccountId,
                transaction_type = cmd.TransactionType,
                amount_minor_units = cmd.AmountMinorUnits,
                currency_code = cmd.CurrencyCode,
                recorded_at = txn.RecordedAt.UtcDateTime.ToString("O"),
            }),
        });
        // SaveChanges is owned by the caller's transaction scope.
        return await Task.FromResult(txn.Id);
    }
}
