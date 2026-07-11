using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Peikon.Transactions.Infrastructure;

/// <summary>
/// A message that can never succeed on redelivery (unparseable envelope, bad
/// ids). It routes straight to the DLQ instead of burning the retry budget.
/// </summary>
public sealed class PoisonEventException(Exception cause)
    : Exception(cause.Message, cause);

/// <summary>
/// The payload does not conform to the schema the producer framed it with.
/// Redelivery cannot fix it — poison, park it with the keyword details.
/// </summary>
public sealed class EventContractViolationException(int schemaId, string details)
    : Exception($"payload violates schema id {schemaId}: {details}");

/// <summary>
/// The registry authoritatively answered 404 for the frame's schema id: the
/// frame references a schema that does not exist, so the frame is junk (poison).
/// </summary>
public sealed class UnknownSchemaIdException(int schemaId)
    : Exception($"schema id {schemaId} unknown to the registry");

/// <summary>
/// The registry could not answer (network failure, timeout, 5xx). Transient:
/// the event must NOT be dead-lettered and its offset must NOT advance — the
/// consumer seeks back and retries, trading liveness for never writing an
/// unvalidated money fact.
/// </summary>
public sealed class SchemaRegistryUnavailableException(int schemaId, Exception cause)
    : Exception($"registry unavailable resolving schema id {schemaId}: {cause.Message}", cause);

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
