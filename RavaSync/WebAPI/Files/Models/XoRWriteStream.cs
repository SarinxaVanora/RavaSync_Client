using System.Buffers;

namespace RavaSync.WebAPI.Files.Models;

public sealed class XorWriteStream : Stream
{
    private readonly Stream _inner;
    private readonly byte _key;
    private bool _disposed;

    public XorWriteStream(Stream inner, byte key)
    {
        _inner = inner;
        _key = key;
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override void Flush() => _inner.Flush();
    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

    public override void Write(byte[] buffer, int offset, int count)
    {
        var pool = ArrayPool<byte>.Shared;
        var tmp = pool.Rent(count);
        try
        {
            Buffer.BlockCopy(buffer, offset, tmp, 0, count);
            for (int i = 0; i < count; i++) tmp[i] ^= _key;
            _inner.Write(tmp, 0, count);
        }
        finally
        {
            pool.Return(tmp);
        }
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var pool = ArrayPool<byte>.Shared;
        var tmp = pool.Rent(buffer.Length);
        try
        {
            buffer.Span.CopyTo(tmp);
            for (int i = 0; i < buffer.Length; i++) tmp[i] ^= _key;
            await _inner.WriteAsync(tmp.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            pool.Return(tmp);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        _disposed = true;
        base.Dispose(disposing);
        // do NOT dispose _inner (owned by HTTP stack)
    }

    // Unused
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
