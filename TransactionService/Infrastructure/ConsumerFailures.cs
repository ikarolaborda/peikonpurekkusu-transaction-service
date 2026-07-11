using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Peikon.Transactions.Infrastructure;

/// <summary>
/// A message that can never succeed on redelivery (unparseable envelope, bad
/// ids). It routes straight to the DLQ instead of burning the retry budget.
/// </summary>
public sealed class PoisonEventException(Exception cause)
    : Exception(cause.Message, cause);

public static class DbUpdateExceptionExtensions
{
    private const string UniqueViolation = "23505";

    /// <summary>
    /// True only for a genuine duplicate key. Every other DbUpdateException
    /// (deadlock, statement timeout, dropped connection) means the row was NOT
    /// written — treating those as "already processed" silently drops events.
    /// </summary>
    public static bool IsUniqueViolation(this DbUpdateException ex) =>
        ex.InnerException is PostgresException { SqlState: UniqueViolation };
}
