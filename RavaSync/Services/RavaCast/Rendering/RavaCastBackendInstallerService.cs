using Dalamud.Plugin;
using Microsoft.Extensions.Logging;
using Microsoft.Win32;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RavaSync.Services.RavaCast.Rendering;

/// <summary>
/// Resolves the bundled WebView2 renderer from the plugin root.
/// The renderer is packaged beside RavaSync.dll, so there is no installer/download flow and no subfolder probing.
/// </summary>
public sealed class RavaCastBackendInstallerService : IDisposable
{
    public const string RendererDownloadUrl = "Bundled with plugin";
    public const string RendererExeName = "RavaCast.Renderer.exe";
    public const string NativeBridgeDllName = "RavaCast.Media.Native.dll";
    public const string BridgeHostExeName = "RavaCast.Media.BridgeHost.exe";
    private const string FirewallRuleBase = "RavaCast Direct Stream BridgeHost";
    private const string LegacyFirewallRuleUdp = "RavaCast Direct Stream BridgeHost UDP";
    private const string LegacyFirewallRuleTcp = "RavaCast Direct Stream BridgeHost TCP";
    private const string WebView2RuntimeClientGuid = "{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}";
    private const string WebView2BootstrapperUrl = "https://go.microsoft.com/fwlink/p/?LinkId=2124703";
    private static readonly string[] RequiredDirectStreamRuntimeFiles =
    [
        NativeBridgeDllName,
        BridgeHostExeName,
        "datachannel.dll",
        "ffmpeg.exe",
        "libcrypto-3-x64.dll",
        "libssl-3-x64.dll"
    ];

    private readonly ILogger<RavaCastBackendInstallerService> _logger;
    private readonly IDalamudPluginInterface _pluginInterface;
    private readonly SemaphoreSlim _webView2InstallGate = new(1, 1);

    public RavaCastBackendInstallerService(ILogger<RavaCastBackendInstallerService> logger, IDalamudPluginInterface pluginInterface)
    {
        _logger = logger;
        _pluginInterface = pluginInterface;
    }

    public string InstallDirectory => PluginBaseDirectory;
    public string DownloadDirectory => Path.Combine(_pluginInterface.ConfigDirectory.FullName, "RavaCast", "Downloads");
    public string RendererPath => Path.Combine(InstallDirectory, RendererExeName);
    public string NativeBridgePath => Path.Combine(InstallDirectory, NativeBridgeDllName);
    public string BridgeHostPath => Path.Combine(InstallDirectory, BridgeHostExeName);
    private string FirewallStatusPath => Path.Combine(DownloadDirectory, "AllowRavaCastDirectStreamFirewall.status.txt");
    private string CurrentFirewallRuleSuffix => BuildFirewallRuleSuffix(BridgeHostPath);
    private string CurrentFirewallRuleUdp => $"{FirewallRuleBase} UDP {CurrentFirewallRuleSuffix}";
    private string CurrentFirewallRuleTcp => $"{FirewallRuleBase} TCP {CurrentFirewallRuleSuffix}";
    public bool IsInstalled => File.Exists(RendererPath);
    public bool IsWebView2RuntimeInstalled => TryGetInstalledWebView2RuntimeVersion(out _);
    public bool IsNativeBridgeInstalled => MissingDirectStreamNativeFiles.Length == 0;
    public string[] MissingRendererFiles => IsInstalled ? [] : [RendererExeName];
    public string[] MissingDirectStreamNativeFiles => RequiredDirectStreamRuntimeFiles.Where(name => !File.Exists(Path.Combine(InstallDirectory, name))).ToArray();
    public bool IsInstalling { get; private set; }
    public string StatusText { get; private set; } = "Bundled WebView2 renderer";
    public string? Detail { get; private set; }
    public double Progress { get; private set; } = 1.0;

    private string PluginBaseDirectory
    {
        get
        {
            try
            {
                var assemblyPath = _pluginInterface.AssemblyLocation?.FullName;
                if (!string.IsNullOrWhiteSpace(assemblyPath))
                {
                    var directory = Path.GetDirectoryName(assemblyPath);
                    if (!string.IsNullOrWhiteSpace(directory) && Directory.Exists(directory))
                        return directory;
                }
            }
            catch
            {
                // Non-fatal probe failure; fallback path below handles it without log noise.
            }

            var fallback = Path.GetDirectoryName(typeof(RavaCastBackendInstallerService).Assembly.Location);
            if (!string.IsNullOrWhiteSpace(fallback) && Directory.Exists(fallback))
                return fallback;

            return _pluginInterface.ConfigDirectory.FullName;
        }
    }

    public bool TryGetInstalledWebView2RuntimeVersion(out string version)
    {
        version = string.Empty;
        if (!OperatingSystem.IsWindows())
            return false;

        foreach (var probe in WebView2RegistryProbes())
        {
            try
            {
                using var baseKey = RegistryKey.OpenBaseKey(probe.Hive, probe.View);
                using var key = baseKey.OpenSubKey(probe.Path);
                var raw = key?.GetValue("pv") as string;
                if (!IsUsableRuntimeVersion(raw)) continue;
                version = raw!.Trim();
                return true;
            }
            catch
            {
                // Registry probing tries multiple locations; failures are expected on some machines.
            }
        }

        return false;
    }

    public async Task<bool> EnsureWebView2RuntimeReadyAsync(CancellationToken token)
    {
        if (!OperatingSystem.IsWindows())
        {
            StatusText = "WebView2 runtime unavailable";
            Detail = "WebView2 Evergreen Runtime installation is only supported for the Windows renderer path.";
            return false;
        }

        if (TryGetInstalledWebView2RuntimeVersion(out var existingVersion))
        {
            StatusText = "WebView2 Evergreen Runtime ready";
            Detail = "Installed runtime version: " + existingVersion;
            Progress = 1.0;
            return true;
        }

        await _webView2InstallGate.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (TryGetInstalledWebView2RuntimeVersion(out existingVersion))
            {
                StatusText = "WebView2 Evergreen Runtime ready";
                Detail = "Installed runtime version: " + existingVersion;
                Progress = 1.0;
                return true;
            }

            IsInstalling = true;
            Progress = 0.05;
            StatusText = "Installing WebView2 Evergreen Runtime";
            Detail = "Downloading Microsoft's Evergreen Bootstrapper for a silent per-user/per-machine install.";
            Directory.CreateDirectory(DownloadDirectory);

            var bootstrapperPath = Path.Combine(DownloadDirectory, "MicrosoftEdgeWebView2Setup.exe");
            await DownloadWebView2BootstrapperAsync(bootstrapperPath, token).ConfigureAwait(false);
            Progress = 0.45;
            Detail = "Running WebView2 Evergreen Runtime installer silently.";

            var psi = new ProcessStartInfo
            {
                FileName = bootstrapperPath,
                Arguments = "/silent /install",
                UseShellExecute = false,
                CreateNoWindow = true,
                WorkingDirectory = DownloadDirectory
            };

            using var process = Process.Start(psi) ?? throw new InvalidOperationException("Could not start MicrosoftEdgeWebView2Setup.exe.");
            await process.WaitForExitAsync(token).ConfigureAwait(false);
            Progress = 0.85;

            if (TryGetInstalledWebView2RuntimeVersion(out var installedVersion) || await WaitForWebView2RuntimeRegistrationAsync(token).ConfigureAwait(false))
            {
                TryGetInstalledWebView2RuntimeVersion(out installedVersion);
                StatusText = "WebView2 Evergreen Runtime ready";
                Detail = string.IsNullOrWhiteSpace(installedVersion) ? "Runtime installed successfully." : "Installed runtime version: " + installedVersion;
                Progress = 1.0;
                // Healthy RavaCast setup path; do not log unless it fails.
                return true;
            }

            StatusText = "WebView2 Evergreen Runtime install failed";
            Detail = $"Installer exited with code {process.ExitCode}, but the runtime registry entry was still missing.";
            _logger.LogWarning("WebView2 Evergreen Runtime installer exited with code {code}, but runtime registration was not detected.", process.ExitCode);
            return false;
        }
        catch (OperationCanceledException)
        {
            StatusText = "WebView2 Evergreen Runtime install cancelled";
            Detail = null;
            throw;
        }
        catch (Exception ex)
        {
            StatusText = "WebView2 Evergreen Runtime install failed";
            Detail = ex.Message;
            _logger.LogWarning(ex, "Failed to install WebView2 Evergreen Runtime for RavaCast");
            return false;
        }
        finally
        {
            IsInstalling = false;
            _webView2InstallGate.Release();
        }
    }

    private static (RegistryHive Hive, RegistryView View, string Path)[] WebView2RegistryProbes()
    {
        var clientPath = @"SOFTWARE\Microsoft\EdgeUpdate\Clients\" + WebView2RuntimeClientGuid;
        var wowClientPath = @"SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients\" + WebView2RuntimeClientGuid;
        return
        [
            (RegistryHive.LocalMachine, RegistryView.Registry32, clientPath),
            (RegistryHive.LocalMachine, RegistryView.Registry64, clientPath),
            (RegistryHive.LocalMachine, RegistryView.Registry64, wowClientPath),
            (RegistryHive.CurrentUser, RegistryView.Registry32, clientPath),
            (RegistryHive.CurrentUser, RegistryView.Registry64, clientPath),
        ];
    }

    private static bool IsUsableRuntimeVersion(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return false;
        var value = raw.Trim();
        if (string.Equals(value, "0.0.0.0", StringComparison.OrdinalIgnoreCase)) return false;
        return Version.TryParse(value.Split(' ', StringSplitOptions.RemoveEmptyEntries)[0], out var parsed) && parsed > new Version(0, 0, 0, 0);
    }

    private static async Task DownloadWebView2BootstrapperAsync(string destinationPath, CancellationToken token)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(2) };
        using var response = await http.GetAsync(WebView2BootstrapperUrl, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        await using var source = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
        await using var destination = new FileStream(destinationPath, FileMode.Create, FileAccess.Write, FileShare.Read, 128 * 1024, useAsync: true);
        await source.CopyToAsync(destination, token).ConfigureAwait(false);
    }

    private async Task<bool> WaitForWebView2RuntimeRegistrationAsync(CancellationToken token)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline)
        {
            token.ThrowIfCancellationRequested();
            if (TryGetInstalledWebView2RuntimeVersion(out _))
                return true;
            await Task.Delay(750, token).ConfigureAwait(false);
        }
        return false;
    }



    public bool IsDirectStreamFirewallAllowed(out string detail)
    {
        detail = string.Empty;
        if (!OperatingSystem.IsWindows())
        {
            detail = "Windows Firewall is not used on this platform.";
            return true;
        }

        if (!File.Exists(BridgeHostPath))
        {
            detail = $"{BridgeHostExeName} is not packaged beside the plugin yet.";
            return false;
        }

        if (TryReadFirewallInstallStatus(out var installedFromStatus, out var statusDetail))
        {
            detail = statusDetail;
            if (installedFromStatus)
                return true;
        }

        var udpRule = CurrentFirewallRuleUdp;
        var tcpRule = CurrentFirewallRuleTcp;
        var udp = TryGetFirewallRule(udpRule, out var udpDetail);
        var tcp = TryGetFirewallRule(tcpRule, out var tcpDetail);
        if (udp && tcp)
        {
            detail = "Direct Stream firewall rules are already present for this BridgeHost.";
            return true;
        }

        var legacyUdp = TryGetFirewallRule(LegacyFirewallRuleUdp, out var legacyUdpDetail);
        var legacyTcp = TryGetFirewallRule(LegacyFirewallRuleTcp, out var legacyTcpDetail);
        if (legacyUdp && legacyTcp)
        {
            detail = "Direct Stream legacy firewall rules are already present for this BridgeHost.";
            return true;
        }

        detail = string.Join(" ", new[] { udpDetail, tcpDetail, legacyUdpDetail, legacyTcpDetail }.Where(x => !string.IsNullOrWhiteSpace(x)));
        return false;
    }

    public bool TryRequestDirectStreamFirewallPermission(out string error)
    {
        error = string.Empty;

        if (!OperatingSystem.IsWindows())
        {
            error = "Windows Firewall rules are only needed on Windows/Wine hosts.";
            return false;
        }

        if (!File.Exists(BridgeHostPath))
        {
            error = $"{BridgeHostExeName} is not packaged beside the plugin yet. Build/publish RavaCast first, then request the firewall rule.";
            return false;
        }

        if (IsDirectStreamFirewallAllowed(out _))
        {
            error = "Direct Stream firewall rules are already allowed.";
            return true;
        }

        try
        {
            Directory.CreateDirectory(DownloadDirectory);
            var scriptPath = Path.Combine(DownloadDirectory, "AllowRavaCastDirectStreamFirewall.ps1");
            var statusPath = FirewallStatusPath;
            var bridgeHostLiteral = EscapePowerShellSingleQuoted(BridgeHostPath);
            var statusLiteral = EscapePowerShellSingleQuoted(statusPath);
            var udpLiteral = EscapePowerShellSingleQuoted(CurrentFirewallRuleUdp);
            var tcpLiteral = EscapePowerShellSingleQuoted(CurrentFirewallRuleTcp);
            var legacyUdpLiteral = EscapePowerShellSingleQuoted(LegacyFirewallRuleUdp);
            var legacyTcpLiteral = EscapePowerShellSingleQuoted(LegacyFirewallRuleTcp);

            var script = string.Join(Environment.NewLine, new[]
            {
                "$ErrorActionPreference = 'Stop'",
                $"$program = '{bridgeHostLiteral}'",
                $"$status = '{statusLiteral}'",
                $"$udpName = '{udpLiteral}'",
                $"$tcpName = '{tcpLiteral}'",
                $"$legacyUdpName = '{legacyUdpLiteral}'",
                $"$legacyTcpName = '{legacyTcpLiteral}'",
                string.Empty,
                "try {",
                "    if (-not (Test-Path -LiteralPath $program)) {",
                "        throw ('RavaCast.Media.BridgeHost.exe was not found at: ' + $program)",
                "    }",
                string.Empty,
                "    foreach ($ruleName in @($udpName, $tcpName, $legacyUdpName, $legacyTcpName)) {",
                "        try { Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue | Remove-NetFirewallRule -ErrorAction SilentlyContinue } catch { }",
                "        try { & netsh advfirewall firewall delete rule name=\"$ruleName\" | Out-Null } catch { }",
                "    }",
                string.Empty,
                "    & netsh advfirewall firewall add rule name=\"$udpName\" dir=in action=allow program=\"$program\" enable=yes profile=any protocol=UDP | Out-Null",
                "    & netsh advfirewall firewall add rule name=\"$tcpName\" dir=in action=allow program=\"$program\" enable=yes profile=any protocol=TCP | Out-Null",
                string.Empty,
                "    Set-Content -LiteralPath $status -Value ('OK|' + (Get-Date).ToString('O') + '|' + $program) -Encoding UTF8",
                "}",
                "catch {",
                "    try {",
                "        Set-Content -LiteralPath $status -Value ('ERROR|' + (Get-Date).ToString('O') + '|' + $_.Exception.Message) -Encoding UTF8",
                "    } catch { }",
                string.Empty,
                "    throw",
                "}"
            });

            File.WriteAllText(scriptPath, script, Encoding.UTF8);

            var psi = new ProcessStartInfo
            {
                FileName = "powershell.exe",
                UseShellExecute = true,
                Verb = "runas",
                WindowStyle = ProcessWindowStyle.Normal,
                Arguments = $"-NoProfile -ExecutionPolicy Bypass -File \"{scriptPath}\""
            };

            Process.Start(psi);
            // Healthy RavaCast firewall request path; do not log unless it fails.
            error = "Windows permission prompt requested. Accept it to allow Direct Stream media connections. If no prompt appeared, the game may already be running elevated; checking firewall rules shortly.";
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to request RavaCast Direct Stream firewall permission");
            error = ex.Message;
            return false;
        }
    }

    private static string EscapePowerShellSingleQuoted(string value) => (value ?? string.Empty).Replace("'", "''");

    private bool TryReadFirewallInstallStatus(out bool installed, out string detail)
    {
        installed = false;
        detail = string.Empty;

        try
        {
            if (!File.Exists(FirewallStatusPath)) return false;

            var text = File.ReadAllText(FirewallStatusPath, Encoding.UTF8).Trim();
            if (string.IsNullOrWhiteSpace(text)) return false;

            var parts = text.Split(new[] { '|' }, 3);
            if (parts.Length == 0) return false;

            if (string.Equals(parts[0], "OK", StringComparison.OrdinalIgnoreCase))
            {
                var program = parts.Length >= 3 ? parts[2] : string.Empty;
                if (FirewallRuleOutputMatchesProgram(program, BridgeHostPath))
                {
                    installed = true;
                    detail = "Direct Stream firewall rules were installed for this BridgeHost.";
                    return true;
                }

                detail = "A Direct Stream firewall install status was found, but it was for a different BridgeHost path.";
                return true;
            }

            if (string.Equals(parts[0], "ERROR", StringComparison.OrdinalIgnoreCase))
            {
                detail = parts.Length >= 3 ? "Direct Stream firewall install failed: " + parts[2] : "Direct Stream firewall install failed.";
                return true;
            }
        }
        catch (Exception ex)
        {
            detail = "Could not read Direct Stream firewall install status: " + ex.Message;
            return true;
        }

        return false;
    }

    private bool TryGetFirewallRule(string ruleName, out string detail)
    {
        detail = string.Empty;
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "netsh.exe",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                Arguments = $"advfirewall firewall show rule name=\"{ruleName}\" verbose"
            };
            using var process = Process.Start(psi);
            if (process is null)
            {
                detail = $"Could not query firewall rule '{ruleName}'.";
                return false;
            }

            if (!process.WaitForExit(2000))
            {
                try { process.Kill(entireProcessTree: true); } catch { }
                detail = $"Firewall rule query timed out for '{ruleName}'.";
                return false;
            }

            var output = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
            if (process.ExitCode != 0 || output.Contains("No rules match", StringComparison.OrdinalIgnoreCase))
            {
                detail = $"Missing firewall rule '{ruleName}'.";
                return false;
            }

            if (!FirewallRuleOutputMatchesProgram(output, BridgeHostPath))
            {
                detail = $"Firewall rule '{ruleName}' exists, but not for this BridgeHost path.";
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            detail = $"Could not query firewall rule '{ruleName}': {ex.Message}";
            return false;
        }
    }


    private static bool FirewallRuleOutputMatchesProgram(string output, string expectedProgramPath)
    {
        if (string.IsNullOrWhiteSpace(output) || string.IsNullOrWhiteSpace(expectedProgramPath)) return false;

        var fullPath = Path.GetFullPath(expectedProgramPath);
        var candidates = new[]
        {
            expectedProgramPath,
            fullPath,
            $"\"{expectedProgramPath}\"",
            $"\"{fullPath}\""
        };

        return candidates.Any(candidate => output.Contains(candidate, StringComparison.OrdinalIgnoreCase));
    }

    private static string BuildFirewallRuleSuffix(string bridgeHostPath)
    {
        try
        {
            var fullPath = Path.GetFullPath(bridgeHostPath ?? string.Empty).Trim().ToUpperInvariant();
            var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(fullPath));
            return Convert.ToHexString(bytes, 0, 4);
        }
        catch
        {
            var fallback = Math.Abs((bridgeHostPath ?? string.Empty).GetHashCode()).ToString("X8");
            return fallback.Length > 8 ? fallback[..8] : fallback.PadLeft(8, '0');
        }
    }

    public void StartInstall()
    {
        IsInstalling = false;
        Progress = 1.0;
        var runtimeInstalled = TryGetInstalledWebView2RuntimeVersion(out var runtimeVersion);
        StatusText = IsInstalled ? "Bundled WebView2 renderer ready" : "Bundled WebView2 renderer missing";
        Detail = IsInstalled
            ? (runtimeInstalled
                ? (IsNativeBridgeInstalled ? $"Renderer, WebView2 Evergreen Runtime {runtimeVersion}, Direct Stream native shim, BridgeHost, and Direct Stream runtime files are ready." : $"Renderer and WebView2 Evergreen Runtime {runtimeVersion} are ready. Direct Stream is missing: {string.Join(", ", MissingDirectStreamNativeFiles)}")
                : "Renderer is packaged, but WebView2 Evergreen Runtime is missing. RavaCast will try to install it silently before launching the browser.")
            : $"{RendererExeName} is missing from the plugin root: {InstallDirectory}";
        // Healthy RavaCast capability checks are intentionally not logged; keep RavaCast logging error-only.
    }

    public void CancelInstall()
    {
        IsInstalling = false;
    }

    public void Dispose()
    {
        try { _webView2InstallGate.Dispose(); } catch { }
    }
}
