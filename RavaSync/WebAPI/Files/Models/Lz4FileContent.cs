using K4os.Compression.LZ4.Legacy;
using System.Buffers;
using System.Net;
using System.Net.Http.Headers;

namespace RavaSync.WebAPI.Files.Models;

public sealed class NonClosingStream : Stream
{
    private readonly Stream _inner;
    public NonClosingStream(Stream inner) => _inner = inner;

    public override bool CanRead => _inner.CanRead;
    public override bool CanSeek => _inner.CanSeek;
    public override bool CanWrite => _inner.CanWrite;
    public override long Length => _inner.Length;
    public override long Position { get => _inner.Position; set => _inner.Position = value; }

    public override void Flush() => _inner.Flush();
    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);
    public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => _inner.ReadAsync(buffer, offset, count, cancellationToken);
    public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
    public override void SetLength(long value) => _inner.SetLength(value);
    public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => _inner.WriteAsync(buffer, offset, count, cancellationToken);

    protected override void Dispose(bool disposing)
    {
        // do NOT close the inner HttpContent write stream
        if (disposing) { try { _inner.Flush(); } catch { } }
    }

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
    public override async ValueTask DisposeAsync()
    {
        try { await _inner.FlushAsync().ConfigureAwait(false); } catch { }
        // do not dispose _inner
    }
#endif
}




public sealed class Lz4FileContent : HttpContent
{
    private readonly string _path;
    private readonly long _fileSize;
    private readonly IProgress<UploadProgress>? _progress;
    private readonly byte? _xorKey; // 42 for munged
    private const int BufferSize = 1 * 1024 * 1024;

    public Lz4FileContent(string path, long fileSize, IProgress<UploadProgress>? progress = null, byte? xorKey = null)
    {
        _path = path;
        _fileSize = fileSize;
        _progress = progress;
        _xorKey = xorKey;
        Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
    }

    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        var fopts = new FileStreamOptions
        {
            Access = FileAccess.Read,
            Mode = FileMode.Open,
            Share = FileShare.Read,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
            BufferSize = BufferSize
        };

        await using var file = new FileStream(_path, fopts);

        // XOR BEFORE compression when munged
        Stream source = file;
        if (_xorKey.HasValue)
            source = new XorReadStream(file, _xorKey.Value);

        // 🔧 KEY: prevent closing HttpClient's chunked writer
        using var nonClosing = new NonClosingStream(stream);

        await using var lz4 = new K4os.Compression.LZ4.Legacy.LZ4Stream(
            nonClosing,
            K4os.Compression.LZ4.Legacy.LZ4StreamMode.Compress,
            K4os.Compression.LZ4.Legacy.LZ4StreamFlags.None,
            BufferSize);

        var pool = ArrayPool<byte>.Shared;
        var buffer = pool.Rent(BufferSize);
        long uploaded = 0;

        try
        {
            int read;
            while ((read = await source.ReadAsync(buffer.AsMemory(0, buffer.Length)).ConfigureAwait(false)) > 0)
            {
                await lz4.WriteAsync(buffer.AsMemory(0, read)).ConfigureAwait(false);
                uploaded += read; // pre-compression progress
                _progress?.Report(new UploadProgress(uploaded, _fileSize));
            }

            await lz4.FlushAsync().ConfigureAwait(false);
        }
        finally
        {
            pool.Return(buffer);
        }
    }

    protected override bool TryComputeLength(out long length)
    {
        length = -1; // streaming, unknown length
        return false;
    }
}

// simple XOR reader used above
file sealed class XorReadStream : Stream
{
    private readonly Stream _inner;
    private readonly byte _key;
    public XorReadStream(Stream inner, byte key) { _inner = inner; _key = key; }
    public override bool CanRead => _inner.CanRead;
    public override bool CanSeek => _inner.CanSeek;
    public override bool CanWrite => false;
    public override long Length => _inner.Length;
    public override long Position { get => _inner.Position; set => _inner.Position = value; }
    public override void Flush() => _inner.Flush();
    public override int Read(byte[] buffer, int offset, int count)
    {
        var r = _inner.Read(buffer, offset, count);
        for (int i = 0; i < r; i++) buffer[offset + i] ^= _key;
        return r;
    }
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var r = await _inner.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
        for (int i = 0; i < r; i++) buffer[offset + i] ^= _key;
        return r;
    }
    public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
    public override void SetLength(long value) => _inner.SetLength(value);
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
