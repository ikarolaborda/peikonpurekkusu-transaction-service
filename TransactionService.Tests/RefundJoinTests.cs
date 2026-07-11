using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Peikon.Transactions.Domain;
using Peikon.Transactions.Infrastructure;
using Xunit;

namespace Peikon.Transactions.Tests;

public class RefundJoinTests
{
    private static TxDbContext NewDb() =>
        new(new DbContextOptionsBuilder<TxDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options);

    private static async Task SeedPurchase(TxDbContext db, string paymentId)
    {
        await new RecordTransactionHandler(db).HandleAsync(
            new RecordTransaction(Guid.CreateVersion7(), paymentId, "acct-9", "user-9", "m-shop",
                "purchase", 5000, "USD", "ledger-cap", "psp-cap", DateTimeOffset.UtcNow),
            CancellationToken.None);
        await db.SaveChangesAsync();
    }

    private static Envelope Reversed(string paymentId, long amount = 5000, string currency = "USD") =>
        new("evt-1", "payments.payment.reversed.v1", 1, DateTimeOffset.UtcNow, "peikon", "corr-1",
            new JsonObject
            {
                ["payment_id"] = paymentId,
                ["amount_minor_units"] = amount,
                ["currency_code"] = currency,
                ["reversal_ledger_transaction_id"] = "ledger-rev",
                ["reason"] = "customer_request",
            });

    [Fact]
    public async Task Refund_inherits_identity_from_the_original_purchase()
    {
        await using var db = NewDb();
        await SeedPurchase(db, "pay-r1");
        var env = Reversed("pay-r1", amount: 1500);

        var cmd = await PaymentFactsConsumer.BuildRefundAsync(db, Guid.CreateVersion7(), env, env.Payload, CancellationToken.None);

        Assert.Equal("refund", cmd.TransactionType);
        Assert.Equal("acct-9", cmd.AccountId);
        Assert.Equal("user-9", cmd.UserId);
        Assert.Equal("m-shop", cmd.MerchantId);
        Assert.Equal(1500, cmd.AmountMinorUnits);        // event amount (partial refund), not the purchase's
        Assert.Equal("ledger-rev", cmd.LedgerTransactionId);
    }

    [Fact]
    public async Task Refund_for_an_unrecorded_purchase_defers_retryable_not_poison()
    {
        await using var db = NewDb();
        var env = Reversed("pay-missing");

        // Retryable (bounded retry -> DLQ), NOT poison: the purchase may still be in flight.
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            PaymentFactsConsumer.BuildRefundAsync(db, Guid.CreateVersion7(), env, env.Payload, CancellationToken.None));
    }

    [Fact]
    public async Task Refund_with_multiple_purchase_rows_is_poison()
    {
        await using var db = NewDb();
        await SeedPurchase(db, "pay-dup");
        await SeedPurchase(db, "pay-dup"); // corruption: two purchases for one payment
        var env = Reversed("pay-dup");

        await Assert.ThrowsAsync<PoisonEventException>(() =>
            PaymentFactsConsumer.BuildRefundAsync(db, Guid.CreateVersion7(), env, env.Payload, CancellationToken.None));
    }
}
