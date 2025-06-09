using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace VideoProcessWorker.Models;

[Table("video")]
public class VideoMetadata
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("user_id")]
    public string UserId { get; set; }

    [Column("file_name")]
    public string FileName { get; set; }

    [Column("s3key")]
    public string S3Key { get; set; }

    [Column("upload_time")]
    public DateTime UploadTime { get; set; }

    [Column("status")]
    public string Status { get; set; } = "pending";

    [Column("thumbnail_url")]
    public string? ThumbnailUrl { get; set; }

    [Column("video_480p_url")]
    public string? Video480pUrl { get; set; }

    [Column("video_720p_url")]
    public string? Video720pUrl { get; set; }
}