using RavaSync.API.Dto.Files;

namespace RavaSync.WebAPI.Files.Models;

public class UploadFileTransfer : FileTransfer
{
    public UploadFileTransfer(UploadFileDto dto) : base(dto)
    {
    }

    public string LocalFile { get; set; } = string.Empty;
    public string? StatusText { get; set; }
    public override long Total { get; set; }
}