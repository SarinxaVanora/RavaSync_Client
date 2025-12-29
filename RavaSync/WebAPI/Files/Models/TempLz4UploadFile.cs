using K4os.Compression.LZ4.Legacy;
using System.Buffers;
using System.Security.Cryptography;

namespace RavaSync.WebAPI.Files.Models;

internal sealed class TempLz4UploadFile : IAsyncDisposable
{
    public string TempPath { get; }
    public long RawSize { get; }
    public long CompressedSize { get; }
    public string Md5Base64 { get; }

    private TempLz4UploadFile(string tempPath, long rawSize, long compressedSize, string md5Base64)
    {
        TempPath = tempPath;
        RawSize = rawSize;
        CompressedSize = compressedSize;
        Md5Base64 = md5Base64;
    }

    public static async Task<TempLz4UploadFile> CreateAsync(string filePath, CancellationToken ct, IProgress<long>? rawReadProgress = null)
    {
        const int BlockSize = 256 * 1024;

        var tempPath = Path.Combine(Path.GetTempPath(), $"ravasync_upload_{Guid.NewGuid():N}.lz4");

        await using var input = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 1024 * 1024, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        await using var outputFile = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 1024 * 1024, options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        using var md5 = MD5.Create();

        // Hashing wrapper so we can compute Content-MD5 without a second pass.
        await using var hashingStream = new HashingWriteStream(outputFile, md5);

        await using var lz4 = new LZ4Stream(hashingStream, LZ4StreamMode.Compress, LZ4StreamFlags.None, BlockSize);

        var buffer = ArrayPool<byte>.Shared.Rent(BlockSize);
        long raw = 0;
        try
        {
            int read;
            while ((read = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false)) > 0)
            {
                await lz4.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
                raw += read;
                rawReadProgress?.Report(raw);
            }

            await lz4.FlushAsync(ct).ConfigureAwait(false);
            await hashingStream.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        hashingStream.FinalizeHash();

        var compressedSize = outputFile.Length;
        var md5Base64 = Convert.ToBase64String(md5.Hash!);

        return new TempLz4UploadFile(tempPath, raw, compressedSize, md5Base64);
    }

    public ValueTask DisposeAsync()
    {
        try { if (File.Exists(TempPath)) File.Delete(TempPath); } catch { }
        return ValueTask.CompletedTask;
    }

    private sealed class HashingWriteStream : Stream
    {
        private readonly Stream _inner;
        private readonly HashAlgorithm _hash;
        private bool _finalized;

        public HashingWriteStream(Stream inner, HashAlgorithm hash)
        {
            _inner = inner;
            _hash = hash;
        }

        public void FinalizeHash()
        {
            if (_finalized) return;
            _hash.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
            _finalized = true;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _hash.TransformBlock(buffer, offset, count, null, 0);
            _inner.Write(buffer, offset, count);
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _hash.TransformBlock(buffer, offset, count, null, 0);
            await _inner.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (buffer.Length > 0)
            {
                var arr = buffer.ToArray();
                _hash.TransformBlock(arr, 0, arr.Length, null, 0);
                return _inner.WriteAsync(arr, cancellationToken);
            }
            return _inner.WriteAsync(buffer, cancellationToken);
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }
        public override void Flush() => _inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        protected override void Dispose(bool disposing) { if (disposing) _inner.Dispose(); base.Dispose(disposing); }
        public override async ValueTask DisposeAsync() { await _inner.DisposeAsync().ConfigureAwait(false); await base.DisposeAsync().ConfigureAwait(false); }
    }
}
