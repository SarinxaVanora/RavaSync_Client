using System;
using System.Security.Cryptography;
using System.Text;

namespace RavaSync.Services.Discovery;

public static class RavaSessionId
{
    private const string SessionSalt = "RavaSync-Discovery-2025-SessionSalt";

    /// <summary>
    /// Derive a stable, non-PII session id from our existing ident (hashed CID).
    /// Every client that sees the same character can derive the same session id.
    /// </summary>
    public static string FromIdent(string ident)
    {
        using var sha = SHA256.Create();
        var raw = Encoding.UTF8.GetBytes($"{SessionSalt}|{ident}");
        var hash = sha.ComputeHash(raw);

        return Convert.ToHexString(hash, 0, 16);
    }
}
