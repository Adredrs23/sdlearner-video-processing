using System.Text.Json.Serialization;

public class VideoMessage
{
    [JsonPropertyName("videoId")]
    public string videoId { get; set; }
}