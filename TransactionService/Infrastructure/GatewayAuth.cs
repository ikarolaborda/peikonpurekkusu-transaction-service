using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Peikon.Transactions.Infrastructure;

/// <summary>
/// Verifies the signed gateway assertion user-service mints at ForwardAuth, so
/// this service trusts identity from a cryptographic claim rather than from a
/// raw X-User-Id — which any peer on the internal network could otherwise forge
/// (Traefik only overwrites it at the edge). The assertion is an ES256 JWT
/// (aud "peikon-internal") signed by user-service, verified against its JWKS.
/// </summary>
public sealed class GatewayAssertionValidator
{
    private const string Audience = "peikon-internal";

    private readonly HttpClient _http;
    private readonly string _jwksUrl;
    private readonly ILogger<GatewayAssertionValidator> _log;
    private readonly JsonWebTokenHandler _handler = new();
    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private IReadOnlyCollection<SecurityKey> _keys = [];

    public GatewayAssertionValidator(HttpClient http, string jwksUrl, ILogger<GatewayAssertionValidator> log)
    {
        _http = http;
        _jwksUrl = jwksUrl;
        _log = log;
    }

    /// <summary>Loads the JWKS at startup, retrying because user-service may not be ready yet.</summary>
    public async Task InitializeAsync()
    {
        for (var attempt = 1; attempt <= 10; attempt++)
        {
            if (await RefreshAsync())
            {
                return;
            }

            _log.LogWarning("gateway JWKS not ready, retrying (attempt {Attempt})", attempt);
            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        throw new InvalidOperationException($"gateway JWKS unreachable after retries: {_jwksUrl}");
    }

    /// <summary>
    /// Returns the verified subject (user id), or null if the assertion is missing,
    /// malformed, expired, wrong-audience, wrong-algorithm, or unverifiable. One JWKS
    /// refresh is attempted on an unknown signing key, so a key rotation self-heals.
    /// </summary>
    public async Task<string?> ValidateAsync(string token)
    {
        var sub = await ValidateOnceAsync(token);
        if (sub is not null)
        {
            return sub;
        }

        if (await RefreshAsync())
        {
            return await ValidateOnceAsync(token);
        }

        return null;
    }

    private async Task<string?> ValidateOnceAsync(string token)
    {
        if (_keys.Count == 0)
        {
            return null;
        }

        var result = await _handler.ValidateTokenAsync(token, new TokenValidationParameters
        {
            ValidateIssuer = false,
            ValidateAudience = true,
            ValidAudience = Audience,
            ValidateLifetime = true,
            ClockSkew = TimeSpan.FromSeconds(10),
            ValidAlgorithms = ["ES256"],
            IssuerSigningKeys = _keys,
        });

        if (!result.IsValid || result.SecurityToken is not JsonWebToken jwt)
        {
            return null;
        }

        var sub = jwt.GetClaim("sub").Value;
        return string.IsNullOrEmpty(sub) ? null : sub;
    }

    private async Task<bool> RefreshAsync()
    {
        await _refreshLock.WaitAsync();
        try
        {
            var json = await _http.GetStringAsync(_jwksUrl);
            _keys = new JsonWebKeySet(json).GetSigningKeys().ToArray();
            return _keys.Count > 0;
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "gateway JWKS fetch failed");
            return false;
        }
        finally
        {
            _refreshLock.Release();
        }
    }
}
