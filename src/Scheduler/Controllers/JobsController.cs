using Batchanator.Core;
using Microsoft.AspNetCore.Mvc;
using Scheduler.Models;
using Scheduler.Services;

namespace Scheduler.Controllers;

[ApiController]
[Route("[controller]")]
public class JobsController : ControllerBase
{
    private readonly FileIngestionService _fileIngestion;
    private readonly DatabaseIngestionService _dbIngestion;
    private readonly BatchIngestionService _batchIngestion;
    private readonly ILogger<JobsController> _logger;

    public JobsController(
        FileIngestionService fileIngestion,
        DatabaseIngestionService dbIngestion,
        BatchIngestionService batchIngestion,
        ILogger<JobsController> logger)
    {
        _fileIngestion = fileIngestion;
        _dbIngestion = dbIngestion;
        _batchIngestion = batchIngestion;
        _logger = logger;
    }

    /// <summary>
    /// Trigger ingestion from a file path.
    /// </summary>
    [HttpPost("{jobType}/trigger/file")]
    public async Task<ActionResult<TriggerResponse>> TriggerFromFile(
        string jobType,
        [FromBody] FileTriggerRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.FilePath))
        {
            return BadRequest(new { error = "FilePath is required" });
        }

        if (!System.IO.File.Exists(request.FilePath))
        {
            return BadRequest(new { error = $"File not found: {request.FilePath}" });
        }

        var jobId = await _fileIngestion.IngestFileAsync(request.FilePath, jobType, cancellationToken);

        return Accepted($"/jobs/{jobId}", new TriggerResponse(jobId, jobType, "Processing"));
    }

    /// <summary>
    /// Trigger ingestion from PendingWork database table.
    /// </summary>
    [HttpPost("{jobType}/trigger/database")]
    public async Task<ActionResult<TriggerResponse>> TriggerFromDatabase(
        string jobType,
        CancellationToken cancellationToken)
    {
        var jobId = await _dbIngestion.IngestPendingWorkAsync(jobType, cancellationToken);

        if (jobId == null)
        {
            return Ok(new { message = $"No pending work found for job type: {jobType}" });
        }

        return Accepted($"/jobs/{jobId}", new TriggerResponse(jobId.Value, jobType, "Processing"));
    }

    /// <summary>
    /// Trigger ingestion from directly submitted work items.
    /// </summary>
    [HttpPost("{jobType}/trigger/items")]
    public async Task<ActionResult<TriggerResponse>> TriggerFromItems(
        string jobType,
        [FromBody] ItemsTriggerRequest request,
        CancellationToken cancellationToken)
    {
        if (request.Items == null || request.Items.Count == 0)
        {
            return BadRequest(new { error = "Items array is required and must not be empty" });
        }

        var jobName = string.IsNullOrWhiteSpace(request.JobName)
            ? $"{jobType}-{DateTime.UtcNow:yyyyMMdd-HHmmss}"
            : request.JobName;

        // Convert to RawWorkItem stream
        var items = ConvertToWorkItems(jobType, request.Items);

        var jobId = await _batchIngestion.IngestAsync(jobName, jobType, items, cancellationToken);

        return Accepted($"/jobs/{jobId}", new TriggerResponse(jobId, jobType, "Processing"));
    }

    private static async IAsyncEnumerable<RawWorkItem> ConvertToWorkItems(
        string jobType,
        List<WorkItemInput> items)
    {
        var index = 0;
        foreach (var item in items)
        {
            index++;
            var idempotencyKey = $"{jobType}:{item.IdempotencyKey}";

            yield return new RawWorkItem(
                SourceRowId: index.ToString(),
                IdempotencyKey: idempotencyKey,
                PayloadJson: item.PayloadJson
            );
        }

        await Task.CompletedTask; // Make it async
    }
}
