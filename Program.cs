
using Amazon.S3;
using Amazon.S3.Model;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

Console.WriteLine("🎬 Video Processor Worker Started...");

// RabbitMQ Setup
var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest",
    Port = 5672
};

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(queue: "video-processing",
                     durable: true,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);

// MinIO (S3-compatible) setup
var s3 = new AmazonS3Client("minioadmin", "minioadmin", new AmazonS3Config
{
    ServiceURL = "http://localhost:9000",
    ForcePathStyle = true,
    UseHttp = true
});

// PostgreSQL setup
var connStr = "Host=localhost;Port=5432;Database=sdlearner;Username=postgres;Password=localdev";
await using var db = new NpgsqlConnection(connStr);
await db.OpenAsync();

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    try
    {
        var payload = JsonSerializer.Deserialize<VideoMessage>(message);
        var videoId = payload?.videoId;

        Console.WriteLine($"📥 Received videoId: {videoId}");

        // Fetch video metadata from DB
        var cmd = new NpgsqlCommand("SELECT * FROM \"Videos\" WHERE \"Id\" = @id", db);
        cmd.Parameters.AddWithValue("id", Guid.Parse(videoId));
        await using var reader = await cmd.ExecuteReaderAsync();

        if (!reader.Read())
        {
            Console.WriteLine("❌ Video not found in database.");
            return;
        }

        var fileName = reader.GetString(reader.GetOrdinal("filename"));
        var userId = reader.GetString(reader.GetOrdinal("userid"));
        await reader.CloseAsync();

        // Build S3 keys and paths
        var rawKey = $"{userId}/{videoId}/{fileName}";
        var processedFileName = Path.GetFileNameWithoutExtension(fileName) + "_processed.mp4";
        var rawPath = Path.Combine("temp/raw", fileName);
        var processedPath = Path.Combine("temp/processed", processedFileName);

        // Download raw video from MinIO
        Console.WriteLine("⬇️  Downloading raw video...");
        var getReq = new GetObjectRequest
        {
            BucketName = "raw-uploads",
            Key = rawKey
        };

        using var s3Response = await s3.GetObjectAsync(getReq);
        await using (var fs = File.Create(rawPath))
        {
            await s3Response.ResponseStream.CopyToAsync(fs);
        }

        // Process with FFmpeg
        Console.WriteLine("🎞️  Processing video with FFmpeg...");
        var ff = Process.Start(new ProcessStartInfo
        {
            FileName = "ffmpeg",
            Arguments = $"-i \"{rawPath}\" -c:v libx264 -preset fast \"{processedPath}\" -y",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        });

        ff.WaitForExit();

        if (ff.ExitCode != 0)
        {
            Console.WriteLine("❌ FFmpeg failed.");
            return;
        }

        // Upload processed video to MinIO
        Console.WriteLine("⬆️  Uploading processed video...");
        await using var uploadStream = File.OpenRead(processedPath);
        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "processed-videos",
            Key = $"{userId}/{videoId}/{processedFileName}",
            InputStream = uploadStream,
            ContentType = "video/mp4"
        });

        // Update DB
        var updateCmd = new NpgsqlCommand("UPDATE \"Videos\" SET \"Status\" = 'processed' WHERE \"Id\" = @id", db);
        updateCmd.Parameters.AddWithValue("id", Guid.Parse(videoId));
        await updateCmd.ExecuteNonQueryAsync();

        Console.WriteLine($"✅ Video {videoId} processed and uploaded.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
    }
};

await channel.BasicConsumeAsync(queue: "video-processing",
                     autoAck: true,
                     consumer: consumer);

Console.WriteLine("📡 Listening for messages. Press [enter] to exit.");
Console.ReadLine();

