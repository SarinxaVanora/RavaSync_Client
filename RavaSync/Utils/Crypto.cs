
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace RavaSync.Utils;

public static class Crypto
{
#pragma warning disable SYSLIB0021 // Type or member is obsolete

    private static readonly Dictionary<(string, ushort), string> _hashListPlayersSHA256 = new();
    private static readonly Dictionary<string, string> _hashListSHA256 = new(StringComparer.Ordinal);
    private static readonly SHA256CryptoServiceProvider _sha256CryptoProvider = new();
    private static readonly Dictionary<ulong, string> _hashListUlongSHA256 = new();
    private static readonly object _shaLock = new();

    /// <summary>
    /// Compute SHA1 hash of a file using a streaming FileStream with FileShare.ReadWrite
    /// so that Penumbra / antivirus / etc. holding the file open does not immediately
    /// cause an IOException.
    /// </summary>
    public static string GetFileHash(this string filePath)
    {
        using SHA1CryptoServiceProvider cryptoProvider = new();

        // Stream the file instead of File.ReadAllBytes and allow other readers/writers
        using var fs = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite);

        var hash = cryptoProvider.ComputeHash(fs);
        return BitConverter
            .ToString(hash)
            .Replace("-", "", StringComparison.Ordinal);
    }

    /// <summary>
    /// SHA256 hash for (playerName, worldId) tuples, cached in memory.
    /// </summary>
    public static string ComputeHash256(this (string, ushort) playerToHash)
    {
        if (_hashListPlayersSHA256.TryGetValue(playerToHash, out var hash))
            return hash;

        var bytes = Encoding.UTF8.GetBytes(playerToHash.Item1 + playerToHash.Item2.ToString());

        lock (_shaLock)
        {
            if (_hashListPlayersSHA256.TryGetValue(playerToHash, out hash))
                return hash;

            var newHash = BitConverter
                .ToString(_sha256CryptoProvider.ComputeHash(bytes))
                .Replace("-", "", StringComparison.Ordinal);

            _hashListPlayersSHA256[playerToHash] = newHash;
            return newHash;
        }
    }


    public static string GetHash256(this ulong value)
    {
        if (_hashListUlongSHA256.TryGetValue(value, out var existing))
            return existing;

        var stringToHash = value.ToString();
        byte[] data = Encoding.UTF8.GetBytes(stringToHash);

        lock (_shaLock)
        {
            // double-check under lock
            if (_hashListUlongSHA256.TryGetValue(value, out existing))
                return existing;

            var hashedData = _sha256CryptoProvider.ComputeHash(data);
            var hashedString = BitConverter.ToString(hashedData).Replace("-", "", StringComparison.Ordinal);

            _hashListUlongSHA256[value] = hashedString;
            _hashListSHA256[stringToHash] = hashedString;

            return hashedString;
        }
    }


    public static string GetHash256(this string stringToHash)
    {
        return GetOrComputeHashSHA256(stringToHash);
    }

    private static string GetOrComputeHashSHA256(string stringToCompute)
    {
        if (_hashListSHA256.TryGetValue(stringToCompute, out var hash))
            return hash;

        var bytes = Encoding.UTF8.GetBytes(stringToCompute);

        lock (_shaLock)
        {
            // double-check under lock
            if (_hashListSHA256.TryGetValue(stringToCompute, out hash))
                return hash;

            var newHash = BitConverter
                .ToString(_sha256CryptoProvider.ComputeHash(bytes))
                .Replace("-", "", StringComparison.Ordinal);

            _hashListSHA256[stringToCompute] = newHash;
            return newHash;
        }
    }


#pragma warning restore SYSLIB0021 // Type or member is obsolete
}
