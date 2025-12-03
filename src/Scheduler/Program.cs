using Batchanator.Core;
using Batchanator.Core.Data;
using Microsoft.EntityFrameworkCore;
using Polly;
using Polly.Extensions.Http;
using Scheduler.Handlers;
using Scheduler.Services;

var builder = WebApplication.CreateBuilder(args);

// Configuration
builder.Services.Configure<BatchanatorOptions>(
    builder.Configuration.GetSection(BatchanatorOptions.SectionName));

var batchanatorConfig = builder.Configuration.GetSection(BatchanatorOptions.SectionName).Get<BatchanatorOptions>() ?? new BatchanatorOptions();

// Database - conditional Sqlite or SQL Server
if (batchanatorConfig.DatabaseProvider == DatabaseProvider.Sqlite)
{
    var sqlitePath = batchanatorConfig.SqlitePath;
    builder.Services.AddDbContextFactory<BatchanatorDbContext>(options =>
        options.UseSqlite($"Data Source={sqlitePath}"));
}
else
{
    var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");
    builder.Services.AddDbContextFactory<BatchanatorDbContext>(options =>
        options.UseSqlServer(connectionString, sql =>
            sql.EnableRetryOnFailure(
                maxRetryCount: 5,
                maxRetryDelay: TimeSpan.FromSeconds(30),
                errorNumbersToAdd: null)));
}

// HTTP Client for API calls with transient fault handling
var apiBaseUrl = builder.Configuration.GetSection("Batchanator:ApiBaseUrl").Value ?? "http://localhost:5000";
builder.Services.AddHttpClient("BatchanatorApi", client =>
{
    client.BaseAddress = new Uri(apiBaseUrl);
    client.Timeout = TimeSpan.FromSeconds(30);
})
.AddPolicyHandler(GetTransientHttpPolicy());

static IAsyncPolicy<HttpResponseMessage> GetTransientHttpPolicy()
{
    return HttpPolicyExtensions
        .HandleTransientHttpError() // HttpRequestException, 5xx, 408
        .Or<TaskCanceledException>() // timeouts
        .WaitAndRetryAsync(
            retryCount: 3,
            sleepDurationProvider: attempt => TimeSpan.FromMilliseconds(100 * Math.Pow(2, attempt)));
}

// Distributed locking
builder.Services.AddSingleton<DistributedLockFactory>();

// Ingestion services
builder.Services.AddScoped<BatchIngestionService>();
builder.Services.AddScoped<FileIngestionService>();
builder.Services.AddScoped<DatabaseIngestionService>();

// Scheduled job handlers
builder.Services.AddSingleton<IScheduledJobHandler, StaleJobCleanupHandler>();
builder.Services.AddSingleton<IScheduledJobHandler, DeadLetterRetryHandler>();
builder.Services.AddSingleton<IScheduledJobHandler, ExpiredLockReleaseHandler>();

foreach (var job in batchanatorConfig.ScheduledIngestionJobs.Where(j => j.Enabled))
{
    var jobType = job.JobType;
    var cronExpression = job.CronExpression;
    builder.Services.AddSingleton<IScheduledJobHandler>(sp =>
        new DatabaseIngestionHandler(
            sp,
            sp.GetRequiredService<ILogger<DatabaseIngestionHandler>>(),
            jobType,
            cronExpression));
}

// Background services
builder.Services.AddHostedService<DispatcherHostedService>();
builder.Services.AddHostedService<CronSchedulerService>();

// Controllers
builder.Services.AddControllers();
builder.Services.AddOpenApi();

var app = builder.Build();

// Apply migrations on startup
using (var scope = app.Services.CreateScope())
{
    var factory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<BatchanatorDbContext>>();
    using var db = factory.CreateDbContext();
    db.Database.Migrate();
}

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();
