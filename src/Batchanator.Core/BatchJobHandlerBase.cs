using System.Text.Json;

namespace Batchanator.Core;

/// <summary>
/// Base class for implementing batch job handlers with automatic JSON serialization.
/// </summary>
/// <typeparam name="TPayload">The strongly-typed payload for this job type.</typeparam>
public abstract class BatchJobHandlerBase<TPayload> : IBatchJobHandler<TPayload>, IBatchJobHandler
    where TPayload : class
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public abstract string JobType { get; }

    public Type PayloadType => typeof(TPayload);

    public abstract Task<BatchJobResult> ProcessAsync(
        TPayload payload,
        string idempotencyKey,
        CancellationToken cancellationToken);

    public async Task<BatchJobResult> ProcessRawAsync(
        string payloadJson,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        TPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize<TPayload>(payloadJson, JsonOptions);
        }
        catch (JsonException ex)
        {
            return BatchJobResult.Failed($"Failed to deserialize payload: {ex.Message}");
        }

        if (payload == null)
        {
            return BatchJobResult.Failed("Payload deserialized to null");
        }

        return await ProcessAsync(payload, idempotencyKey, cancellationToken);
    }
}
