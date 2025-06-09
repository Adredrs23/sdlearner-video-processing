namespace VideoProcessWorker.Models;

public class Video
{
    public Guid Id { get; set; }
    public string UserId { get; set; }
    public string FileName { get; set; }
    public string Status { get; set; }
}
