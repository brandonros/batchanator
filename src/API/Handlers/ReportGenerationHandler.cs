using System.Text.Json;
using Batchanator.Core;

namespace API.Handlers;

public class ReportGenerationPayload
{
    public string ReportType { get; set; } = default!;
    public string UserId { get; set; } = default!;
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public string OutputFormat { get; set; } = "pdf"; // "pdf", "csv", "xlsx"
    public Dictionary<string, string>? Parameters { get; set; }
}

public class ReportGenerationHandler : BatchJobHandlerBase<ReportGenerationPayload>
{
    private readonly ILogger<ReportGenerationHandler> _logger;

    public ReportGenerationHandler(ILogger<ReportGenerationHandler> logger)
    {
        _logger = logger;
    }

    public override string JobType => "report-generation";

    public override async Task<BatchJobResult> ProcessAsync(
        ReportGenerationPayload payload,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Generating {ReportType} report for user {UserId} ({StartDate:d} to {EndDate:d}) with key {IdempotencyKey}",
            payload.ReportType, payload.UserId, payload.StartDate, payload.EndDate, idempotencyKey);

        // Validate output format
        if (payload.OutputFormat is not ("pdf" or "csv" or "xlsx"))
        {
            return BatchJobResult.Failed($"Invalid output format: {payload.OutputFormat}. Must be pdf, csv, or xlsx.");
        }

        // Validate date range
        if (payload.EndDate < payload.StartDate)
        {
            return BatchJobResult.Failed("EndDate must be greater than or equal to StartDate");
        }

        // Simulate report generation (500ms - 2s depending on date range)
        var daySpan = (payload.EndDate - payload.StartDate).Days;
        var processingTime = Math.Min(500 + (daySpan * 10), 2000);
        await Task.Delay(processingTime, cancellationToken);

        // Simulate 92% success rate
        if (Random.Shared.NextDouble() < 0.08)
        {
            return BatchJobResult.Failed("Report generation failed: insufficient data for the specified period");
        }

        // Generate mock file reference
        var fileId = Guid.NewGuid().ToString("N");
        var result = new
        {
            generatedAt = DateTime.UtcNow,
            reportType = payload.ReportType,
            outputFormat = payload.OutputFormat,
            fileId = fileId,
            downloadUrl = $"/reports/{fileId}.{payload.OutputFormat}",
            fileSizeBytes = Random.Shared.Next(10000, 500000),
            processingTimeMs = processingTime
        };

        return BatchJobResult.Succeeded(JsonSerializer.Serialize(result));
    }
}
