namespace Batchanator.Core;

/// <summary>
/// Registry of supported job types for database ingestion.
/// </summary>
public static class JobTypeRegistry
{
    public static readonly string[] SupportedJobTypes =
    [
        "email-notification",
        "data-sync",
        "report-generation"
    ];
}
