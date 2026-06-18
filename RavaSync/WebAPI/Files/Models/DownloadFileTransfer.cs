using RavaSync.API.Dto.Files;

namespace RavaSync.WebAPI.Files.Models;

public class DownloadFileTransfer : FileTransfer
{
    public DownloadFileTransfer(DownloadFileDto dto) : base(dto)
    {
    }

    public override bool CanBeTransferred=> Dto.FileExists&& !Dto.IsForbidden&& !string.IsNullOrWhiteSpace(Dto.Url)&& (Dto.Size > 0 || Dto.RawSize > 0);

    public Uri DownloadUri => new(Dto.Url);

    public override long Total
    {
        set
        {
            if (value > 0)
                Dto.Size = value;
        }
        get
        {
            if (Dto.Size > 0) return Dto.Size;
            if (Dto.RawSize > 0) return Dto.RawSize;
            return 0;
        }
    }

    public long TotalRaw => Dto.RawSize;
    public bool IsRawPayload => FilePayloadEncoding.IsRaw(Dto.PayloadEncoding);
    public string PayloadEncoding => Dto.PayloadEncoding;
    private DownloadFileDto Dto => (DownloadFileDto)TransferDto;
}
