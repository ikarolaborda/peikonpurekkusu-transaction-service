using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Peikon.Transactions.Infrastructure;

/// <summary>Platform event envelope (contracts/events/README.md).</summary>
public sealed record Envelope(
    string EventId,
    string EventType,
    int SchemaVersion,
    DateTimeOffset OccurredAt,
    string TenantId,
    string CorrelationId,
    JsonObject Payload);

/// <summary>
/// Confluent wire format (magic 0x00 + big-endian int32 schema id + JSON)
/// against the Apicurio ccompat endpoint — schema ids resolved per topic and
/// cached. Mirrors the Go/Node implementations; the vendor serdes are not
/// compatible with Apicurio's ccompat lookup surface.
/// </summary>
public sealed class EventsCodec(HttpClient http, string registryBaseUrl)
{
    private readonly ConcurrentDictionary<string, int> _ids = new();

    public static Envelope? TryUnframe(byte[] value)
    {
        if (value.Length < 6 || value[0] != 0) return null;
        try
        {
            var node = JsonNode.Parse(value.AsSpan(5).ToArray()) as JsonObject;
            if (node is null) return null;
            var eventId = (string?)node["event_id"];
            var eventType = (string?)node["event_type"];
            if (eventId is null || eventType is null) return null;
            return new Envelope(
                eventId,
                eventType,
                (int?)node["schema_version"] ?? 1,
                DateTimeOffset.TryParse((string?)node["occurred_at"], out var at) ? at : DateTimeOffset.UtcNow,
                (string?)node["tenant_id"] ?? "peikon",
                (string?)node["correlation_id"] ?? eventId,
                node["payload"] as JsonObject ?? []);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public async Task<byte[]> FrameAsync(string topic, object envelope, CancellationToken ct)
    {
        var id = await SchemaIdAsync(topic, ct);
        var payload = JsonSerializer.SerializeToUtf8Bytes(envelope);
        var buf = new byte[5 + payload.Length];
        buf[0] = 0;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1, 4), id);
        payload.CopyTo(buf, 5);
        return buf;
    }

    private async Task<int> SchemaIdAsync(string topic, CancellationToken ct)
    {
        if (_ids.TryGetValue(topic, out var cached)) return cached;
        var resp = await http.GetAsync($"{registryBaseUrl}/subjects/{topic}-value/versions/latest", ct);
        resp.EnsureSuccessStatusCode();
        using var doc = await JsonDocument.ParseAsync(await resp.Content.ReadAsStreamAsync(ct), cancellationToken: ct);
        var id = doc.RootElement.GetProperty("id").GetInt32();
        _ids[topic] = id;
        return id;
    }
}
