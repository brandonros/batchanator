namespace Batchanator.Core;

/// <summary>
/// Defines a handler for processing batch job items of a specific payload type.
/// </summary>
/// <typeparam name="TPayload">The strongly-typed payload for this job type.</typeparam>
public interface IBatchJobHandler<TPayload> where TPayload : class
{
    /// <summary>
    /// The unique identifier for this job type. Must match the jobType used when triggering jobs.
    /// </summary>
    string JobType { get; }

    /// <summary>
    /// Processes a single item from the batch.
    /// </summary>
    /// <param name="payload">The deserialized payload.</param>
    /// <param name="idempotencyKey">Unique key for idempotent processing.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Result indicating success or failure.</returns>
    Task<BatchJobResult> ProcessAsync(TPayload payload, string idempotencyKey, CancellationToken cancellationToken);
}

/// <summary>
/// Non-generic interface for runtime type resolution.
/// </summary>
public interface IBatchJobHandler
{
    string JobType { get; }
    Type PayloadType { get; }
    Task<BatchJobResult> ProcessRawAsync(string payloadJson, string idempotencyKey, CancellationToken cancellationToken);
}

/// <summary>
/// Result of processing a batch job item.
/// </summary>
public record BatchJobResult
{
    public bool Success { get; init; }
    public string? Error { get; init; }
    public string? ResultPayload { get; init; }

    public static BatchJobResult Succeeded(string? resultPayload = null) =>
        new() { Success = true, ResultPayload = resultPayload };

    public static BatchJobResult Failed(string error) =>
        new() { Success = false, Error = error };
}
