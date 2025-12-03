using System.Text.Json;
using Batchanator.Core;

namespace API.Handlers;

public class EmailNotificationPayload
{
    public string To { get; set; } = default!;
    public string Subject { get; set; } = default!;
    public string Body { get; set; } = default!;
    public string? TemplateId { get; set; }
    public Dictionary<string, string>? TemplateVariables { get; set; }
}

public class EmailNotificationHandler : BatchJobHandlerBase<EmailNotificationPayload>
{
    private readonly ILogger<EmailNotificationHandler> _logger;

    public EmailNotificationHandler(ILogger<EmailNotificationHandler> logger)
    {
        _logger = logger;
    }

    public override string JobType => "email-notification";

    public override async Task<BatchJobResult> ProcessAsync(
        EmailNotificationPayload payload,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing email notification to {To} with key {IdempotencyKey}",
            payload.To, idempotencyKey);

        // Simulate email sending (100-300ms)
        await Task.Delay(Random.Shared.Next(100, 300), cancellationToken);

        // Simulate 95% success rate
        if (Random.Shared.NextDouble() < 0.05)
        {
            return BatchJobResult.Failed("Email service temporarily unavailable");
        }

        var result = new
        {
            sentAt = DateTime.UtcNow,
            recipient = payload.To,
            messageId = Guid.NewGuid().ToString("N")
        };

        return BatchJobResult.Succeeded(JsonSerializer.Serialize(result));
    }
}
