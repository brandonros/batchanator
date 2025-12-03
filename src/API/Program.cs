using System.Text.Json;
using API.Handlers;
using Batchanator.Core;
using Batchanator.Core.Data;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Configuration
var batchanatorConfig = builder.Configuration.GetSection(BatchanatorOptions.SectionName).Get<BatchanatorOptions>() ?? new BatchanatorOptions();

// Database - conditional Sqlite or SQL Server
if (batchanatorConfig.DatabaseProvider == DatabaseProvider.Sqlite)
{
    var sqlitePath = batchanatorConfig.SqlitePath;
    builder.Services.AddDbContext<BatchanatorDbContext>(options =>
        options.UseSqlite($"Data Source={sqlitePath}"));
}
else
{
    var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");
    builder.Services.AddDbContext<BatchanatorDbContext>(options =>
        options.UseSqlServer(connectionString));
}

// Register job handlers
builder.Services.AddScoped<IBatchJobHandler, EmailNotificationHandler>();
builder.Services.AddScoped<IBatchJobHandler, DataSyncHandler>();
builder.Services.AddScoped<IBatchJobHandler, ReportGenerationHandler>();

builder.Services.AddOpenApi();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// ============================================
// Process Endpoint - Routes to appropriate handler
// ============================================
app.MapPost("/process", async (ProcessRequest request, IEnumerable<IBatchJobHandler> handlers, ILogger<Program> logger) =>
{
    var handler = handlers.FirstOrDefault(h => h.JobType == request.JobType);

    if (handler == null)
    {
        logger.LogWarning("No handler found for job type {JobType}", request.JobType);
        return Results.Ok(new ProcessResponse(false, $"No handler registered for job type: {request.JobType}", null));
    }

    try
    {
        var result = await handler.ProcessRawAsync(request.Payload, request.IdempotencyKey, CancellationToken.None);
        return Results.Ok(new ProcessResponse(result.Success, result.Error, result.ResultPayload));
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Unhandled error processing item {IdempotencyKey} with handler {JobType}",
            request.IdempotencyKey, request.JobType);
        return Results.Ok(new ProcessResponse(false, $"Unhandled error: {ex.Message}", null));
    }
})
.WithName("ProcessItem")
.WithOpenApi();

// ============================================
// Handler Discovery Endpoint
// ============================================
app.MapGet("/handlers", (IEnumerable<IBatchJobHandler> handlers) =>
{
    var handlerInfo = handlers.Select(h => new HandlerInfo(h.JobType, h.PayloadType.Name));
    return Results.Ok(handlerInfo);
})
.WithName("GetHandlers")
.WithOpenApi();

// ============================================
// Status Endpoints
// ============================================

// GET /jobs - List all jobs
app.MapGet("/jobs", async (BatchanatorDbContext db) =>
{
    var jobs = await db.Jobs
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
        .ToListAsync();

    return Results.Ok(jobs);
})
.WithName("GetJobs")
.WithOpenApi();

// GET /jobs/{id} - Get job details
app.MapGet("/jobs/{id:guid}", async (Guid id, BatchanatorDbContext db) =>
{
    var job = await db.Jobs
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
            db.Batches.Where(b => b.JobId == j.Id).Sum(b => b.TotalItems),
            db.Batches.Where(b => b.JobId == j.Id).Sum(b => b.SucceededItems),
            db.Batches.Where(b => b.JobId == j.Id).Sum(b => b.FailedItems)))
        .FirstOrDefaultAsync();

    return job is null ? Results.NotFound() : Results.Ok(job);
})
.WithName("GetJob")
.WithOpenApi();

// GET /jobs/{id}/batches - Get batches for a job
app.MapGet("/jobs/{id:guid}/batches", async (Guid id, BatchanatorDbContext db) =>
{
    var batches = await db.Batches
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
        .ToListAsync();

    return Results.Ok(batches);
})
.WithName("GetJobBatches")
.WithOpenApi();

// GET /batches/{id}/items - Get items in a batch
app.MapGet("/batches/{id:guid}/items", async (Guid id, BatchanatorDbContext db, int? skip, int? take) =>
{
    var query = db.BatchItems
        .Where(i => i.BatchId == id)
        .OrderBy(i => i.SourceRowId);

    var items = await query
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
        .ToListAsync();

    return Results.Ok(items);
})
.WithName("GetBatchItems")
.WithOpenApi();

app.Run();

// ============================================
// Request/Response Records
// ============================================
record ProcessRequest(string JobType, string IdempotencyKey, string Payload);
record ProcessResponse(bool Success, string? Error, string? Result);

record HandlerInfo(string JobType, string PayloadTypeName);
record JobSummary(Guid Id, string Name, string JobType, string Status, int TotalBatches, int CompletedBatches, DateTime CreatedAt, DateTime? CompletedAt);
record JobDetails(Guid Id, string Name, string JobType, string Status, int TotalBatches, int CompletedBatches, DateTime CreatedAt, DateTime? CompletedAt, int TotalItems, int SucceededItems, int FailedItems);
record BatchSummary(Guid Id, int SequenceNumber, string Status, int TotalItems, int SucceededItems, int FailedItems, DateTime CreatedAt, DateTime? CompletedAt);
record BatchItemSummary(Guid Id, string SourceRowId, string IdempotencyKey, string Status, int AttemptCount, string? LastError, DateTime CreatedAt, DateTime? UpdatedAt);
