using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Signers;

namespace Streamline.Client;

/// <summary>
/// Verifies <c>streamline-attest</c> headers on consumed records using a local
/// Ed25519 public key. No network calls are made.
/// </summary>
/// <example>
/// <code>
/// var pubKey = loadEd25519PublicKeyBytes(); // 32 bytes
/// var verifier = new StreamlineVerifier(pubKey);
///
/// var result = verifier.Verify(consumerRecord);
/// if (result.Verified)
///     Console.WriteLine($"Verified from {result.ProducerId}");
/// </code>
/// </example>
public sealed class StreamlineVerifier
{
    /// <summary>Kafka header name carrying the attestation envelope.</summary>
    public const string AttestHeader = "streamline-attest";

    private readonly byte[] _publicKey;

    /// <summary>
    /// Creates a verifier backed by the given raw Ed25519 public key (32 bytes).
    /// </summary>
    /// <param name="publicKey">Raw 32-byte Ed25519 public key.</param>
    /// <exception cref="ArgumentException">Thrown when key is not 32 bytes.</exception>
    public StreamlineVerifier(byte[] publicKey)
    {
        if (publicKey.Length != 32)
            throw new ArgumentException("Ed25519 public key must be 32 bytes", nameof(publicKey));
        _publicKey = (byte[])publicKey.Clone();
    }

    /// <summary>
    /// Verify the attestation on a consumer record.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <typeparam name="TValue">The record value type.</typeparam>
    /// <param name="record">The consumed record to verify.</param>
    /// <returns>A <see cref="VerificationResult"/> indicating success or failure.</returns>
    public VerificationResult Verify<TKey, TValue>(ConsumerRecord<TKey, TValue> record)
    {
        var headerValue = record.Headers.GetString(AttestHeader);
        if (string.IsNullOrEmpty(headerValue))
            return VerificationResult.Failed();

        AttestationEnvelope? envelope;
        try
        {
            var decoded = Convert.FromBase64String(headerValue);
            envelope = JsonSerializer.Deserialize<AttestationEnvelope>(decoded);
        }
        catch
        {
            return VerificationResult.Failed();
        }

        if (envelope is null)
            return VerificationResult.Failed();

        var canonical = string.Join("|",
            envelope.Topic,
            envelope.Partition,
            envelope.Offset,
            envelope.PayloadSha256,
            envelope.SchemaId,
            envelope.TimestampMs,
            envelope.KeyId);

        byte[] signatureBytes;
        try
        {
            signatureBytes = Convert.FromBase64String(envelope.Signature);
        }
        catch
        {
            return VerificationResult.Failed();
        }

        bool verified;
        try
        {
            var pubKeyParams = new Ed25519PublicKeyParameters(_publicKey);
            var signer = new Ed25519Signer();
            signer.Init(forSigning: false, pubKeyParams);
            var canonicalBytes = Encoding.UTF8.GetBytes(canonical);
            signer.BlockUpdate(canonicalBytes, 0, canonicalBytes.Length);
            verified = signer.VerifySignature(signatureBytes);
        }
        catch
        {
            verified = false;
        }

        return new VerificationResult(
            Verified: verified,
            ProducerId: envelope.KeyId,
            SchemaId: envelope.SchemaId != 0 ? envelope.SchemaId : null,
            ContractId: envelope.ContractId,
            TimestampMs: envelope.TimestampMs);
    }

    private sealed class AttestationEnvelope
    {
        [JsonPropertyName("payload_sha256")]
        public string PayloadSha256 { get; set; } = "";

        [JsonPropertyName("topic")]
        public string Topic { get; set; } = "";

        [JsonPropertyName("partition")]
        public int Partition { get; set; }

        [JsonPropertyName("offset")]
        public long Offset { get; set; }

        [JsonPropertyName("schema_id")]
        public int SchemaId { get; set; }

        [JsonPropertyName("timestamp_ms")]
        public long TimestampMs { get; set; }

        [JsonPropertyName("key_id")]
        public string KeyId { get; set; } = "";

        [JsonPropertyName("signature")]
        public string Signature { get; set; } = "";

        [JsonPropertyName("contract_id")]
        public string? ContractId { get; set; }
    }
}

/// <summary>
/// The result of an attestation verification.
/// </summary>
/// <param name="Verified">Whether the Ed25519 signature was valid.</param>
/// <param name="ProducerId">The key_id from the attestation envelope.</param>
/// <param name="SchemaId">Schema id (null when zero / absent).</param>
/// <param name="ContractId">Optional contract id.</param>
/// <param name="TimestampMs">Attestation timestamp in epoch milliseconds.</param>
public record VerificationResult(
    bool Verified,
    string ProducerId,
    int? SchemaId,
    string? ContractId,
    long TimestampMs)
{
    internal static VerificationResult Failed() =>
        new(Verified: false, ProducerId: "", SchemaId: null, ContractId: null, TimestampMs: 0);
}
