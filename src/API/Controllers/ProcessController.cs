using API.Models;
using Batchanator.Core;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

[ApiController]
[Route("[controller]")]
public class ProcessController : ControllerBase
{
    private readonly IEnumerable<IBatchJobHandler> _handlers;
    private readonly ILogger<ProcessController> _logger;

    public ProcessController(
        IEnumerable<IBatchJobHandler> handlers,
        ILogger<ProcessController> logger)
    {
        _handlers = handlers;
        _logger = logger;
    }

    [HttpPost]
    public async Task<ActionResult<ProcessResponse>> Process(
        [FromBody] ProcessRequest request,
        CancellationToken cancellationToken)
    {
        var handler = _handlers.FirstOrDefault(h => h.JobType == request.JobType);

        if (handler == null)
        {
            _logger.LogWarning("No handler found for job type {JobType}", request.JobType);
            return Ok(new ProcessResponse(false, $"No handler registered for job type: {request.JobType}", null));
        }

        try
        {
            var result = await handler.ProcessRawAsync(request.Payload, request.IdempotencyKey, cancellationToken);
            return Ok(new ProcessResponse(result.Success, result.Error, result.ResultPayload));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled error processing item {IdempotencyKey} with handler {JobType}",
                request.IdempotencyKey, request.JobType);
            return Ok(new ProcessResponse(false, $"Unhandled error: {ex.Message}", null));
        }
    }

    [HttpGet("/handlers")]
    public ActionResult<IEnumerable<HandlerInfo>> GetHandlers()
    {
        var handlerInfo = _handlers.Select(h => new HandlerInfo(h.JobType, h.PayloadType.Name));
        return Ok(handlerInfo);
    }
}
