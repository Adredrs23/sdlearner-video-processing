using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using VideoProcessWorker.Models;
using DotNetEnv;
using Microsoft.Extensions.Configuration;

Env.Load();

await Main();

async Task Main()
{

    var config = new ConfigurationBuilder()
        .AddEnvironmentVariables()
        .Build();

    Console.WriteLine("🎬 Video Processor Worker Started...");

    var s3 = new AmazonS3Client(config["Minio:AccessKey"], config["Minio:SecretKey"], new AmazonS3Config
    {
        ServiceURL = config["Minio:Endpoint"] ?? "http://localhost:9000",
        ForcePathStyle = true,
        UseHttp = true
    });

    // var factory = new ConnectionFactory() { HostName = "localhost" };
    var factory = new ConnectionFactory()
    {
        HostName = config["RabbitMQ:HostName"] ?? "localhost",
        UserName = config["RabbitMQ:UserName"] ?? "Guest",
        Password = config["RabbitMQ:Password"] ?? "Guest",
        Port = int.Parse(config["RabbitMQ:Port"] ?? "5672")
    };
    using var connection = await factory.CreateConnectionAsync();
    using var channel = await connection.CreateChannelAsync();

    await channel.QueueDeclareAsync("video-processing", durable: true, exclusive: false, autoDelete: false);

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (model, ea) =>
    {
        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
        var payload = JsonSerializer.Deserialize<VideoMessage>(message);

        Console.WriteLine($"📥 Processing videoId: {payload.videoId}");

        using var db = new AppDbContext(config);
        var video = await db.Videos.FirstOrDefaultAsync(v => v.Id.ToString() == payload.videoId);
        if (video == null)
        {
            Console.WriteLine("❌ Video not found");
            return;
        }

        var root = Directory.GetCurrentDirectory();
        var tempRawDir = Path.Combine(root, "temp", "raw", payload.videoId);
        var tempProcessedDir = Path.Combine(root, "temp", "processed", payload.videoId);
        Directory.CreateDirectory(tempRawDir);
        Directory.CreateDirectory(tempProcessedDir);

        var inputPath = Path.Combine(tempRawDir, video.FileName);
        var getRequest = new GetObjectRequest { BucketName = "raw-uploads", Key = video.S3Key };
        using (var response = await s3.GetObjectAsync(getRequest))
        using (var fs = new FileStream(inputPath, FileMode.Create))
            await response.ResponseStream.CopyToAsync(fs);

        var thumbnailPath = Path.Combine(tempProcessedDir, "thumb.jpg");
        var video480p = Path.Combine(tempProcessedDir, "video_480p.mp4");
        var video720p = Path.Combine(tempProcessedDir, "video_720p.mp4");


        var ffmpegTasks = new[]
        {
            new { Cmd = $"-threads 2 -ss 00:00:05 -i \"{inputPath}\" -vframes 1 -q:v 2 \"{thumbnailPath}\"" },
            new { Cmd = $"-threads 2 -i \"{inputPath}\" -vf scale=-2:480 \"{video480p}\"" },
            new { Cmd = $"-threads 2 -i \"{inputPath}\" -vf scale=-2:720 \"{video720p}\"" }
        };

        await Parallel.ForEachAsync(ffmpegTasks, async (task, _) =>
        {
            RunFFmpeg(task.Cmd);
        });
        // RunFFmpeg($"-ss 00:00:05 -i \"{inputPath}\" -vframes 1 -q:v 2 \"{thumbnailPath}\"");
        // RunFFmpeg($"-i \"{inputPath}\" -vf scale=-2:480 \"{video480p}\"");
        // RunFFmpeg($"-i \"{inputPath}\" -vf scale=-2:720 \"{video720p}\"");

        async Task UploadFile(string path, string bucket, string key)
        {
            using var fs = new FileStream(path, FileMode.Open);
            var putReq = new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                InputStream = fs
            };
            await s3.PutObjectAsync(putReq);
        }

        var baseKey = $"{video.UserId}/{payload.videoId}";
        await UploadFile(thumbnailPath, "processed-videos", $"{baseKey}/thumb.jpg");
        await UploadFile(video480p, "processed-videos", $"{baseKey}/video_480p.mp4");
        await UploadFile(video720p, "processed-videos", $"{baseKey}/video_720p.mp4");

        video.ThumbnailUrl = $"processed-videos/{baseKey}/thumb.jpg";
        video.Video480pUrl = $"processed-videos/{baseKey}/video_480p.mp4";
        video.Video720pUrl = $"processed-videos/{baseKey}/video_720p.mp4";
        video.Status = "processed";
        await db.SaveChangesAsync();

        try
        {
            Directory.Delete(tempRawDir, true);
            Directory.Delete(tempProcessedDir, true);
        }
        catch (Exception cleanupEx)
        {
            Console.WriteLine($"⚠️ Cleanup failed: {cleanupEx.Message}");
        }

        Console.WriteLine("✅ Video processed & uploaded");
    };

    await channel.BasicConsumeAsync(queue: "video-processing", autoAck: true, consumer: consumer);
    Console.WriteLine("📡 Worker running... Press [enter] to exit.");
    Console.ReadLine();
}

// ---------- Support classes and methods ----------

string RunFFmpeg(string args)
{
    var sw = Stopwatch.StartNew();
    var process = new Process
    {
        StartInfo = new ProcessStartInfo
        {
            FileName = "ffmpeg",
            Arguments = args,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        }
    };
    process.Start();
    process.WaitForExit();

    sw.Stop();

    Console.WriteLine($"⏱ FFmpeg finished in {sw.Elapsed.TotalSeconds:F2} seconds for: {args}");
    return process.StandardError.ReadToEnd();
}



// public class AppDbContext : DbContext
// {
//     public DbSet<VideoMetadata> Videos { get; set; }

//     protected override void OnConfiguring(DbContextOptionsBuilder options)
//         => options.UseNpgsql(config["ConnectionStrings:DefaultConnection"]);
// }


public class AppDbContext : DbContext
{
    private readonly IConfiguration _config;

    public DbSet<VideoMetadata> Videos { get; set; }

    public AppDbContext(IConfiguration config)
    {
        _config = config;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseNpgsql(_config.GetConnectionString("DefaultConnection"));
}