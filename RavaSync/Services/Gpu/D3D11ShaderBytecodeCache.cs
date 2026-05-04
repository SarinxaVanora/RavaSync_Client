using Microsoft.Extensions.Logging;
using RavaSync.MareConfiguration;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace RavaSync.Services.Gpu;

public sealed class D3D11ShaderBytecodeCache
{
    private const string CacheVersion = "ravasync-d3d11-shader-cache-v1";

    private readonly ILogger<D3D11ShaderBytecodeCache> _logger;
    private readonly string _cacheDirectory;
    private readonly ConcurrentDictionary<string, Lazy<byte[]>> _memoryCache = new(StringComparer.Ordinal);

    public D3D11ShaderBytecodeCache(ILogger<D3D11ShaderBytecodeCache> logger, MareConfigService configService)
    {
        _logger = logger;
        _cacheDirectory = Path.Combine(configService.ConfigurationDirectory, "GpuShaderCache");
    }

    public byte[] GetOrCompile(string cacheKey, string shaderSource, Func<byte[]> compile)
    {
        if (string.IsNullOrWhiteSpace(cacheKey)) throw new ArgumentNullException(nameof(cacheKey));
        if (shaderSource == null) throw new ArgumentNullException(nameof(shaderSource));
        if (compile == null) throw new ArgumentNullException(nameof(compile));

        var sourceHash = ComputeHash(CacheVersion + "\n" + cacheKey + "\n" + shaderSource);
        var fullKey = cacheKey + ":" + sourceHash;
        var lazy = _memoryCache.GetOrAdd(fullKey, _ => new Lazy<byte[]>(() => LoadOrCompile(cacheKey, sourceHash, compile), true));
        return lazy.Value;
    }

    private byte[] LoadOrCompile(string cacheKey, string sourceHash, Func<byte[]> compile)
    {
        try
        {
            Directory.CreateDirectory(_cacheDirectory);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not create D3D11 shader bytecode cache directory; compiling {shaderKey} without persistence.", cacheKey);
            return compile();
        }

        var filePath = Path.Combine(_cacheDirectory, SanitizeFileSegment(cacheKey) + "_" + sourceHash + ".cso");
        try
        {
            if (File.Exists(filePath))
            {
                var cached = File.ReadAllBytes(filePath);
                if (cached.Length > 0)
                    return cached;
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Ignoring unreadable D3D11 shader bytecode cache file {path}.", filePath);
        }

        var compiled = compile();
        if (compiled.Length == 0)
            return compiled;

        try
        {
            var tempPath = filePath + "." + Guid.NewGuid().ToString("N") + ".tmp";
            File.WriteAllBytes(tempPath, compiled);
            File.Move(tempPath, filePath, true);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not persist D3D11 shader bytecode cache file {path}.", filePath);
        }

        return compiled;
    }

    private static string ComputeHash(string value)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(bytes).ToLowerInvariant()[..16];
    }

    private static string SanitizeFileSegment(string value)
    {
        var builder = new StringBuilder(value.Length);
        foreach (var ch in value)
        {
            builder.Append(char.IsLetterOrDigit(ch) || ch == '-' || ch == '_' || ch == '.' ? ch : '_');
        }

        var result = builder.ToString().Trim('_');
        return string.IsNullOrWhiteSpace(result) ? "shader" : result;
    }
}
