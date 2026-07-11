using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;

namespace Peikon.Transactions.Infrastructure;

/// <summary>
/// Validates consumed events against the exact schema the producer framed them
/// with (the Confluent frame's schema id), fetched from the registry and cached
/// compiled, forever — registry ids are immutable. Validating by id rather than
/// against a build-time copy keeps additive producer evolution from being
/// dead-lettered by a stale consumer (the schemas use additionalProperties:false).
/// </summary>
public sealed class EventContractValidator(HttpClient http, string registryBaseUrl, ILogger<EventContractValidator> log)
{
    private readonly ConcurrentDictionary<int, JsonSchema> _compiled = new();

    // Formats (uuid, date-time) are deliberately NOT enforced yet: producers
    // have not been sampled for strict RFC conformance, and a format reject
    // would dead-letter otherwise-sound money facts. Types/required/shape are on.
    private static readonly EvaluationOptions Options = new()
    {
        OutputFormat = OutputFormat.List,
        RequireFormatValidation = false,
    };

    /// <exception cref="EventContractViolationException">payload does not conform (poison)</exception>
    /// <exception cref="UnknownSchemaIdException">registry authoritatively does not know the id (poison)</exception>
    /// <exception cref="SchemaRegistryUnavailableException">registry unreachable (transient — retry, never DLQ)</exception>
    public async Task ValidateAsync(int schemaId, JsonObject envelope, CancellationToken ct)
    {
        var schema = _compiled.TryGetValue(schemaId, out var cached)
            ? cached
            : _compiled.GetOrAdd(schemaId, await FetchAsync(schemaId, ct));

        var results = schema.Evaluate(JsonSerializer.SerializeToElement(envelope), Options);
        if (results.IsValid) return;

        var details = (results.Details ?? [])
            .Where(d => d.Errors is { Count: > 0 })
            .SelectMany(d => d.Errors!.Select(e => $"{d.InstanceLocation} {e.Key}: {e.Value}"))
            .Take(5);
        throw new EventContractViolationException(schemaId, string.Join("; ", details));
    }

    private async Task<JsonSchema> FetchAsync(int schemaId, CancellationToken ct)
    {
        HttpResponseMessage resp;
        try
        {
            resp = await http.GetAsync($"{registryBaseUrl}/schemas/ids/{schemaId}", ct);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException && !ct.IsCancellationRequested)
        {
            throw new SchemaRegistryUnavailableException(schemaId, ex);
        }

        using (resp)
        {
            // Only a definitive answer from the registry itself may condemn the
            // frame; anything else (5xx, proxy noise) is treated as an outage.
            if (resp.StatusCode == HttpStatusCode.NotFound) throw new UnknownSchemaIdException(schemaId);
            if (!resp.IsSuccessStatusCode)
            {
                throw new SchemaRegistryUnavailableException(
                    schemaId, new HttpRequestException($"registry answered {(int)resp.StatusCode}"));
            }

            using var doc = await JsonDocument.ParseAsync(await resp.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
            var schema = JsonSchema.FromText(doc.RootElement.GetProperty("schema").GetString()!);
            log.LogInformation("schema id {SchemaId} fetched and compiled", schemaId);
            return schema;
        }
    }
}
