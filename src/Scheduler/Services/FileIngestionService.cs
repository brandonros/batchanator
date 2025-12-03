using System.Text.Json;
using Batchanator.Core;

namespace Scheduler.Services;

/// <summary>
/// Ingests work items from NDJSON files into the batch processing system.
/// Delegates core logic to BatchIngestionService.
/// </summary>
public class FileIngestionService
{
    private readonly BatchIngestionService _batchIngestion;
    private readonly ILogger<FileIngestionService> _logger;

    public FileIngestionService(
        BatchIngestionService batchIngestion,
        ILogger<FileIngestionService> logger)
    {
        _batchIngestion = batchIngestion;
        _logger = logger;
    }

    public async Task<Guid> IngestFileAsync(
        string filePath,
        string jobType,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Ingesting file {FilePath} as job type {JobType}", filePath, jobType);

        var items = ReadFileAsWorkItems(filePath, jobType, cancellationToken);

        return await _batchIngestion.IngestAsync(
            jobName: Path.GetFileName(filePath),
            jobType: jobType,
            items: items,
            cancellationToken: cancellationToken);
    }

    private async IAsyncEnumerable<RawWorkItem> ReadFileAsWorkItems(
        string filePath,
        string jobType,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var lineNumber = 0;

        await using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream);

        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            lineNumber++;

            var idempotencyKey = ExtractIdempotencyKey(line, lineNumber, jobType);

            yield return new RawWorkItem(
                SourceRowId: lineNumber.ToString(),
                IdempotencyKey: idempotencyKey,
                PayloadJson: line
            );
        }
    }

    private static string ExtractIdempotencyKey(string payloadJson, int lineNumber, string jobType)
    {
        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty("idempotency_key", out var keyElement))
            {
                throw new InvalidOperationException(
                    $"Row {lineNumber} missing required 'idempotency_key' field. Every row must include an idempotency_key.");
            }

            var key = keyElement.GetString();
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new InvalidOperationException(
                    $"Row {lineNumber} has empty 'idempotency_key' field. The idempotency_key must be a non-empty string.");
            }

            // Prefix with jobType for namespace separation
            return $"{jobType}:{key}";
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Row {lineNumber} contains invalid JSON: {ex.Message}", ex);
        }
    }
}
