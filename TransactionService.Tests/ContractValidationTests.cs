using System.Buffers.Binary;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging.Abstractions;
using Peikon.Transactions.Infrastructure;
using Xunit;

namespace Peikon.Transactions.Tests;

public class ContractValidationTests
{
    // Mirrors the load-bearing constraints of payment-captured.schema.json:
    // amount_minor_units is required and an integer, unknown fields rejected.
    // The full real schema is exercised by the live smoke probe.
    private const string SchemaText = """
        {
          "$schema": "https://json-schema.org/draft/2020-12/schema",
          "type": "object",
          "properties": {
            "event_id": { "type": "string" },
            "payload": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "amount_minor_units": { "type": "integer" },
                "currency_code": { "type": "string" }
              },
              "required": ["amount_minor_units", "currency_code"]
            }
          },
          "required": ["event_id", "payload"]
        }
        """;

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public int Calls;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(respond(request));
        }
    }

    private static HttpResponseMessage RegistryOk() => new(HttpStatusCode.OK)
    {
        Content = new StringContent(JsonSerializer.Serialize(new { schema = SchemaText })),
    };

    private static EventContractValidator Validator(StubHandler handler) =>
        new(new HttpClient(handler), "http://registry", NullLogger<EventContractValidator>.Instance);

    private static JsonObject Envelope(bool withAmount)
    {
        var payload = new JsonObject { ["currency_code"] = "USD" };
        if (withAmount) payload["amount_minor_units"] = 1250;
        return new JsonObject { ["event_id"] = "e-1", ["payload"] = payload };
    }

    [Fact]
    public async Task It_accepts_a_conforming_payload()
    {
        var validator = Validator(new StubHandler(_ => RegistryOk()));
        await validator.ValidateAsync(14, Envelope(withAmount: true), CancellationToken.None);
    }

    [Fact]
    public async Task It_rejects_a_payload_missing_amount_minor_units_naming_the_field()
    {
        var validator = Validator(new StubHandler(_ => RegistryOk()));
        var ex = await Assert.ThrowsAsync<EventContractViolationException>(
            () => validator.ValidateAsync(14, Envelope(withAmount: false), CancellationToken.None));
        Assert.Contains("amount_minor_units", ex.Message);
    }

    [Fact]
    public async Task It_treats_an_authoritative_404_as_an_unknown_schema_id()
    {
        var validator = Validator(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.NotFound)));
        await Assert.ThrowsAsync<UnknownSchemaIdException>(
            () => validator.ValidateAsync(999, Envelope(withAmount: true), CancellationToken.None));
    }

    [Fact]
    public async Task It_treats_a_5xx_as_a_transient_registry_outage()
    {
        var validator = Validator(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)));
        await Assert.ThrowsAsync<SchemaRegistryUnavailableException>(
            () => validator.ValidateAsync(14, Envelope(withAmount: true), CancellationToken.None));
    }

    [Fact]
    public async Task It_treats_a_network_failure_as_a_transient_registry_outage()
    {
        var validator = Validator(new StubHandler(_ => throw new HttpRequestException("connection refused")));
        await Assert.ThrowsAsync<SchemaRegistryUnavailableException>(
            () => validator.ValidateAsync(14, Envelope(withAmount: true), CancellationToken.None));
    }

    [Fact]
    public async Task It_fetches_each_schema_id_once_and_validates_from_the_compiled_cache()
    {
        var handler = new StubHandler(_ => RegistryOk());
        var validator = Validator(handler);

        await validator.ValidateAsync(14, Envelope(withAmount: true), CancellationToken.None);
        await validator.ValidateAsync(14, Envelope(withAmount: true), CancellationToken.None);

        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task A_transient_outage_does_not_poison_the_cache()
    {
        var fail = true;
        var handler = new StubHandler(_ =>
            fail ? new HttpResponseMessage(HttpStatusCode.InternalServerError) : RegistryOk());
        var validator = Validator(handler);

        await Assert.ThrowsAsync<SchemaRegistryUnavailableException>(
            () => validator.ValidateAsync(14, Envelope(withAmount: true), CancellationToken.None));

        fail = false;
        await validator.ValidateAsync(14, Envelope(withAmount: true), CancellationToken.None);
    }

    [Fact]
    public void TryParseFrame_extracts_the_schema_id_the_producer_framed_with()
    {
        var body = """{"event_id":"e-1","payload":{}}"""u8.ToArray();
        var frame = new byte[5 + body.Length];
        frame[0] = 0;
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1, 4), 14);
        body.CopyTo(frame, 5);

        var parsed = EventsCodec.TryParseFrame(frame);
        Assert.NotNull(parsed);
        Assert.Equal(14, parsed.Value.SchemaId);
        Assert.Equal("e-1", (string?)parsed.Value.Node["event_id"]);
    }

    [Fact]
    public void TryParseFrame_rejects_a_frame_without_the_magic_byte()
    {
        Assert.Null(EventsCodec.TryParseFrame("{\"event_id\":\"e-1\"}"u8.ToArray()));
    }
}
