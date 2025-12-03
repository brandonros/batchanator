namespace Scheduler.Handlers;

public interface IScheduledJobHandler
{
    string Name { get; }
    string CronExpression { get; }
    Task ExecuteAsync(CancellationToken cancellationToken);
}
