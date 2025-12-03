namespace Batchanator.Core;

/// <summary>
/// Represents a work item ready for ingestion into the batch processing system.
/// This is the common data contract between different ingestion sources (file, database, etc.)
/// </summary>
public record RawWorkItem(
    string SourceRowId,      // e.g., line number, DB row ID
    string IdempotencyKey,   // already extracted/computed, includes jobType prefix
    string PayloadJson       // the work payload
);
