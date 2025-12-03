using Batchanator.Core.Entities;
using Batchanator.Core.Enums;
using Microsoft.EntityFrameworkCore;

namespace Batchanator.Core.Data;

public class BatchanatorDbContext : DbContext
{
    public BatchanatorDbContext(DbContextOptions<BatchanatorDbContext> options)
        : base(options)
    {
    }

    public DbSet<Job> Jobs => Set<Job>();
    public DbSet<Batch> Batches => Set<Batch>();
    public DbSet<BatchItem> BatchItems => Set<BatchItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Job configuration
        modelBuilder.Entity<Job>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).HasMaxLength(500).IsRequired();
            entity.Property(e => e.JobType).HasMaxLength(100).IsRequired();
            entity.Property(e => e.Status)
                .HasConversion<string>()
                .HasMaxLength(50);

            entity.HasIndex(e => e.Status);
            entity.HasIndex(e => e.CreatedAt);
        });

        // Batch configuration
        modelBuilder.Entity<Batch>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Status)
                .HasConversion<string>()
                .HasMaxLength(50);

            entity.HasOne(e => e.Job)
                .WithMany(j => j.Batches)
                .HasForeignKey(e => e.JobId)
                .OnDelete(DeleteBehavior.Cascade);

            entity.HasIndex(e => e.JobId);
            entity.HasIndex(e => e.Status);
        });

        // BatchItem configuration
        modelBuilder.Entity<BatchItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.SourceRowId).HasMaxLength(500).IsRequired();
            entity.Property(e => e.IdempotencyKey).HasMaxLength(500).IsRequired();
            entity.Property(e => e.Status)
                .HasConversion<string>()
                .HasMaxLength(50);
            entity.Property(e => e.LockedBy).HasMaxLength(200);

            entity.HasOne(e => e.Batch)
                .WithMany(b => b.Items)
                .HasForeignKey(e => e.BatchId)
                .OnDelete(DeleteBehavior.Cascade);

            // Critical indexes for the batch processing pattern
            entity.HasIndex(e => e.IdempotencyKey).IsUnique();
            entity.HasIndex(e => e.BatchId);
            entity.HasIndex(e => new { e.Status, e.NextAttemptAt, e.LockedUntil })
                .HasDatabaseName("IX_BatchItems_Claiming");
        });
    }
}
