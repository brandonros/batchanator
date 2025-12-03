using API.Models;
using Batchanator.Core.Data;
using Batchanator.Core.Enums;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace API.Controllers;

[ApiController]
[Route("[controller]")]
public class JobsController : ControllerBase
{
    private readonly BatchanatorDbContext _db;

    public JobsController(BatchanatorDbContext db)
    {
        _db = db;
    }

    [HttpGet]
    public async Task<ActionResult<IEnumerable<JobSummary>>> GetJobs(CancellationToken cancellationToken)
    {
        var jobs = await _db.Jobs
            .OrderByDescending(j => j.CreatedAt)
            .Take(100)
            .Select(j => new JobSummary(
                j.Id,
                j.Name,
                j.JobType,
                j.Status.ToString(),
                j.TotalBatches,
                j.CompletedBatches,
                j.CreatedAt,
                j.CompletedAt))
            .ToListAsync(cancellationToken);

        return Ok(jobs);
    }

    [HttpGet("{id:guid}")]
    public async Task<ActionResult<JobDetails>> GetJob(Guid id, CancellationToken cancellationToken)
    {
        var job = await _db.Jobs
            .Where(j => j.Id == id)
            .Select(j => new JobDetails(
                j.Id,
                j.Name,
                j.JobType,
                j.Status.ToString(),
                j.TotalBatches,
                j.CompletedBatches,
                j.CreatedAt,
                j.CompletedAt,
                _db.Batches.Where(b => b.JobId == j.Id).Sum(b => b.TotalItems),
                _db.Batches.Where(b => b.JobId == j.Id).Sum(b => b.SucceededItems),
                _db.Batches.Where(b => b.JobId == j.Id).Sum(b => b.FailedItems)))
            .FirstOrDefaultAsync(cancellationToken);

        if (job is null)
            return NotFound();

        return Ok(job);
    }

    [HttpGet("{id:guid}/batches")]
    public async Task<ActionResult<IEnumerable<BatchSummary>>> GetJobBatches(
        Guid id,
        CancellationToken cancellationToken)
    {
        var batches = await _db.Batches
            .Where(b => b.JobId == id)
            .OrderBy(b => b.SequenceNumber)
            .Select(b => new BatchSummary(
                b.Id,
                b.SequenceNumber,
                b.Status.ToString(),
                b.TotalItems,
                b.SucceededItems,
                b.FailedItems,
                b.CreatedAt,
                b.CompletedAt))
            .ToListAsync(cancellationToken);

        return Ok(batches);
    }

    [HttpGet("/batches/{id:guid}/items")]
    public async Task<ActionResult<IEnumerable<BatchItemSummary>>> GetBatchItems(
        Guid id,
        [FromQuery] int? skip,
        [FromQuery] int? take,
        CancellationToken cancellationToken)
    {
        var items = await _db.BatchItems
            .Where(i => i.BatchId == id)
            .OrderBy(i => i.SourceRowId)
            .Skip(skip ?? 0)
            .Take(take ?? 100)
            .Select(i => new BatchItemSummary(
                i.Id,
                i.SourceRowId,
                i.IdempotencyKey,
                i.Status.ToString(),
                i.AttemptCount,
                i.LastError,
                i.CreatedAt,
                i.UpdatedAt))
            .ToListAsync(cancellationToken);

        return Ok(items);
    }

    [HttpPost("{id:guid}/cancel")]
    public async Task<ActionResult<CancelResponse>> CancelJob(Guid id, CancellationToken cancellationToken)
    {
        // Cancel all non-terminal items (Pending, Failed, Processing)
        var cancelledCount = await _db.BatchItems
            .Where(i => i.Batch.JobId == id &&
                        (i.Status == BatchItemStatus.Pending ||
                         i.Status == BatchItemStatus.Failed ||
                         i.Status == BatchItemStatus.Processing))
            .ExecuteUpdateAsync(s => s
                .SetProperty(i => i.Status, BatchItemStatus.Cancelled)
                .SetProperty(i => i.UpdatedAt, DateTime.UtcNow)
                .SetProperty(i => i.LockedBy, (string?)null)
                .SetProperty(i => i.LockedUntil, (DateTime?)null), cancellationToken);

        // Update job status
        var job = await _db.Jobs.FindAsync([id], cancellationToken);
        if (job != null)
        {
            job.Status = JobStatus.Cancelled;
            job.CompletedAt = DateTime.UtcNow;
            await _db.SaveChangesAsync(cancellationToken);
        }

        return Ok(new CancelResponse(cancelledCount));
    }

    [HttpGet("{id:guid}/dead-letter")]
    public async Task<ActionResult<List<DeadLetterItem>>> GetDeadLetterItems(
        Guid id,
        CancellationToken cancellationToken)
    {
        var items = await _db.BatchItems
            .Where(i => i.Batch.JobId == id && i.Status == BatchItemStatus.DeadLetter)
            .Select(i => new DeadLetterItem(i.Id, i.IdempotencyKey, i.LastError, i.AttemptCount, i.UpdatedAt))
            .ToListAsync(cancellationToken);

        return Ok(items);
    }

    [HttpPost("{id:guid}/dead-letter/replay")]
    public async Task<ActionResult<ReplayResponse>> ReplayDeadLetterItems(
        Guid id,
        CancellationToken cancellationToken)
    {
        var replayedCount = await _db.BatchItems
            .Where(i => i.Batch.JobId == id && i.Status == BatchItemStatus.DeadLetter)
            .ExecuteUpdateAsync(s => s
                .SetProperty(i => i.Status, BatchItemStatus.Failed)
                .SetProperty(i => i.NextAttemptAt, DateTime.UtcNow)
                .SetProperty(i => i.UpdatedAt, DateTime.UtcNow), cancellationToken);

        return Ok(new ReplayResponse(replayedCount));
    }

    [HttpDelete("{id:guid}/dead-letter")]
    public async Task<ActionResult<PurgeResponse>> PurgeDeadLetterItems(
        Guid id,
        CancellationToken cancellationToken)
    {
        var purgedCount = await _db.BatchItems
            .Where(i => i.Batch.JobId == id && i.Status == BatchItemStatus.DeadLetter)
            .ExecuteDeleteAsync(cancellationToken);

        return Ok(new PurgeResponse(purgedCount));
    }
}
