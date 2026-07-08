using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace Peikon.Transactions.Infrastructure;

/// <summary>Immutable transaction record — the query-side truth of what happened.</summary>
public sealed class Transaction
{
    public Guid Id { get; init; } = Guid.CreateVersion7();
    public required string PaymentId { get; init; }
    public required string AccountId { get; init; }
    public required string UserId { get; init; }
    public required string MerchantId { get; init; }
    public required string Type { get; init; }
    public required long AmountMinorUnits { get; init; }
    public required string CurrencyCode { get; init; }
    public required string LedgerTransactionId { get; init; }
    public required string PspReference { get; init; }
    public DateTimeOffset OccurredAt { get; init; }
    public DateTimeOffset RecordedAt { get; init; } = DateTimeOffset.UtcNow;
}

public sealed class ProcessedEvent
{
    public required Guid EventId { get; init; }
    public DateTimeOffset ProcessedAt { get; init; } = DateTimeOffset.UtcNow;
}

public sealed class OutboxEvent
{
    public Guid Id { get; init; } = Guid.CreateVersion7();
    public required string AggregateType { get; init; }
    public required string AggregateId { get; init; }
    public required string Type { get; init; }
    public required JsonDocument Payload { get; init; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? ProcessedAt { get; set; }
}

public sealed class TxDbContext(DbContextOptions<TxDbContext> options) : DbContext(options)
{
    public DbSet<Transaction> Transactions => Set<Transaction>();
    public DbSet<ProcessedEvent> ProcessedEvents => Set<ProcessedEvent>();
    public DbSet<OutboxEvent> Outbox => Set<OutboxEvent>();

    /// <summary>
    /// Applied after EnsureCreated: append-only enforcement at the database —
    /// UPDATE/DELETE on transactions raises, matching the ledger's stance.
    /// </summary>
    public const string ImmutabilityTriggers = """
        create or replace function forbid_txn_mutation() returns trigger as $$
        begin
          raise exception 'transactions are immutable (append-only): % on %', tg_op, tg_table_name;
        end $$ language plpgsql;
        drop trigger if exists transactions_immutable on transactions;
        create trigger transactions_immutable
          before update or delete on transactions
          for each row execute function forbid_txn_mutation();
        """;

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Transaction>(e =>
        {
            e.ToTable("transactions");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.PaymentId).HasColumnName("payment_id");
            e.Property(x => x.AccountId).HasColumnName("account_id");
            e.Property(x => x.UserId).HasColumnName("user_id");
            e.Property(x => x.MerchantId).HasColumnName("merchant_id");
            e.Property(x => x.Type).HasColumnName("transaction_type");
            e.Property(x => x.AmountMinorUnits).HasColumnName("amount");
            e.Property(x => x.CurrencyCode).HasColumnName("currency");
            e.Property(x => x.LedgerTransactionId).HasColumnName("ledger_transaction_id");
            e.Property(x => x.PspReference).HasColumnName("psp_reference");
            e.Property(x => x.OccurredAt).HasColumnName("occurred_at");
            e.Property(x => x.RecordedAt).HasColumnName("recorded_at");
            e.HasIndex(x => new { x.UserId, x.RecordedAt });
            e.HasIndex(x => x.PaymentId);
        });

        b.Entity<ProcessedEvent>(e =>
        {
            e.ToTable("processed_events");
            e.HasKey(x => x.EventId);
            e.Property(x => x.EventId).HasColumnName("event_id");
            e.Property(x => x.ProcessedAt).HasColumnName("processed_at");
        });

        b.Entity<OutboxEvent>(e =>
        {
            e.ToTable("outbox");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.AggregateType).HasColumnName("aggregatetype");
            e.Property(x => x.AggregateId).HasColumnName("aggregateid");
            e.Property(x => x.Type).HasColumnName("type");
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("jsonb");
            e.Property(x => x.CreatedAt).HasColumnName("created_at");
            e.Property(x => x.ProcessedAt).HasColumnName("processed_at");
            e.HasIndex(x => x.ProcessedAt).HasFilter("processed_at is null");
        });
    }
}
