using Scheduler.Services;

namespace Scheduler.Handlers;

public class DatabaseIngestionHandler : IScheduledJobHandler
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<DatabaseIngestionHandler> _logger;
    private readonly string _jobType;
    private readonly string _cronExpression;

    public DatabaseIngestionHandler(
        IServiceProvider serviceProvider,
        ILogger<DatabaseIngestionHandler> logger,
        string jobType,
        string cronExpression)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _jobType = jobType;
        _cronExpression = cronExpression;
    }

    public string Name => $"DatabaseIngestion-{_jobType}";
    public string CronExpression => _cronExpression;

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var ingestionService = scope.ServiceProvider.GetRequiredService<DatabaseIngestionService>();

        var jobId = await ingestionService.IngestPendingWorkAsync(_jobType, cancellationToken);
        if (jobId != null)
        {
            _logger.LogInformation("Created job {JobId} from database for {JobType}", jobId, _jobType);
        }
    }
}
