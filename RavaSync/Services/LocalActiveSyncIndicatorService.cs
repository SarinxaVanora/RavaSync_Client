using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FFXIVClientStructs.FFXIV.Client.Game.Character;
using Microsoft.Extensions.Logging;
using RavaSync.Interop.Ipc;
using RavaSync.Services.Mediator;

namespace RavaSync.Services;

public sealed class LocalActiveSyncIndicatorService : DisposableMediatorSubscriberBase
{
    private const int PendingAnimationBindGraceMs = 900;
    private const int ClearAfterMissedAnimationChecks = 2;
    private const int MinimumSongLengthSeconds = 20;
    private const int MaximumEstimatedSongLengthSeconds = 600;
    private const long MinimumSongLikeScdBytes = 2L * 1024L * 1024L;
    private readonly DalamudUtilService _dalamudUtil;
    private readonly object _lock = new();
    private readonly Dictionary<nint, LocalSoundAnimationAnchor> _activeSoundAnchors = new();
    private readonly Dictionary<string, TimeSpan?> _songLengthCache = new(StringComparer.OrdinalIgnoreCase);
    private bool _isPlayingSound;

    public LocalActiveSyncIndicatorService(ILogger<LocalActiveSyncIndicatorService> logger, MareMediator mediator, DalamudUtilService dalamudUtil, IpcManager ipcManager)
        : base(logger, mediator)
    {
        _dalamudUtil = dalamudUtil;

        Mediator.Subscribe<PenumbraResourceLoadMessage>(this, HandlePenumbraResourceLoad);
        Mediator.Subscribe<PriorityFrameworkUpdateMessage>(this, _ => ValidateLocalSoundAnimationAnchors());
        Mediator.Subscribe<ZoneSwitchStartMessage>(this, _ => Clear());
        Mediator.Subscribe<PenumbraDisposedMessage>(this, _ => Clear());
        Mediator.Subscribe<DisconnectedMessage>(this, _ => Clear());
    }

    // VFX indicators were intentionally removed. Audio is the only local activity marker now.
    public bool IsUsingVfx => false;

    public bool IsPlayingSound
    {
        get
        {
            lock (_lock)
                return _isPlayingSound;
        }
    }

    private void HandlePenumbraResourceLoad(PenumbraResourceLoadMessage msg)
    {
        if (!_dalamudUtil.IsOnFrameworkThread)
        {
            _ = _dalamudUtil.RunOnFrameworkThread(() => HandlePenumbraResourceLoad(msg));
            return;
        }

        if (msg.GameObject == IntPtr.Zero)
            return;

        var address = (nint)msg.GameObject;
        if (!IsLocalOwnedAddress(address))
            return;

        var gamePath = NormalizePath(msg.GamePath);
        var filePath = NormalizePath(msg.FilePath);
        if (!IsLiveSoundPath(gamePath) && !IsLiveSoundPath(filePath))
            return;

        var longLived = IsLongLivedSoundPath(gamePath) || IsLongLivedSoundPath(filePath);
        TimeSpan? songLength = null;
        if (!longLived && TryGetSongLength(msg.FilePath, out var detectedSongLength))
            songLength = detectedSongLength;

        MarkSoundObserved(address, longLived, songLength);
    }

    private void MarkSoundObserved(nint address, bool longLived, TimeSpan? songLength)
    {
        var now = Environment.TickCount64;

        bool changed;
        lock (_lock)
        {
            if (longLived)
            {
                // Preserve the existing loop/music behaviour exactly: long-lived sounds are anchored
                // to the local animation/action lifecycle and are not shortened by a timer.
                _activeSoundAnchors[address] = CaptureLocalSoundAnimationAnchor(address, now);
            }
            else if (songLength.HasValue)
            {
                // Some mods play a whole song from a non-looping SCD. Those should show as active
                // music while the song is expected to be audible, but still clear when the animation
                // ends or when the estimated song duration has passed. Tiny one-shot sounds stay ignored.
                _activeSoundAnchors[address] = CaptureLocalSoundAnimationAnchor(address, now, songLength.Value);
            }
            else
            {
                // Non-loop / one-shot SCDs are intentionally not held. If the sound is too short
                // to naturally register in the UI, it does not need a sticky activity marker.
                return;
            }

            changed = RefreshPlayingSoundStateUnsafe(now);
        }

        if (changed)
            PublishRefresh();
    }

    private void ValidateLocalSoundAnimationAnchors()
    {
        if (!_dalamudUtil.IsOnFrameworkThread)
        {
            _ = _dalamudUtil.RunOnFrameworkThread(ValidateLocalSoundAnimationAnchors);
            return;
        }

        List<nint> activeAddresses;
        lock (_lock)
        {
            if (_activeSoundAnchors.Count == 0)
                return;

            activeAddresses = _activeSoundAnchors.Keys.ToList();
        }

        var now = Environment.TickCount64;
        var localOwnedAddresses = GetLocalOwnedAddresses();
        var removedAny = false;

        foreach (var address in activeAddresses)
        {
            if (!localOwnedAddresses.Contains(address))
            {
                removedAny |= RemoveAnchor(address);
                continue;
            }

            var current = CaptureLocalAnimationSignature(address);
            LocalSoundAnimationAnchor? anchor;
            lock (_lock)
            {
                _activeSoundAnchors.TryGetValue(address, out anchor);
            }

            if (anchor == null)
                continue;

            if (anchor.IsExpired(now))
            {
                removedAny |= RemoveAnchor(address);
                continue;
            }

            if (!anchor.IsBound)
            {
                if (current.IsAnchorable)
                {
                    RebindAnchor(address, current, now);
                    continue;
                }

                if (unchecked(anchor.PendingBindUntilTick - now) > 0)
                    continue;

                removedAny |= RemoveAnchor(address);
                continue;
            }

            if (anchor.IsStillActive(current))
            {
                ResetAnchorMisses(address);
                continue;
            }

            if (RegisterAnchorMiss(address) >= ClearAfterMissedAnimationChecks)
                removedAny |= RemoveAnchor(address);
        }

        if (removedAny)
            RefreshPlayingSoundState();
    }

    private HashSet<nint> GetLocalOwnedAddresses()
    {
        var output = new HashSet<nint>();
        var player = (nint)_dalamudUtil.GetPlayerPtr();
        if (player == nint.Zero)
            return output;

        output.Add(player);
        AddIfPresent(output, (nint)_dalamudUtil.GetCompanionPtr((IntPtr)player));
        AddIfPresent(output, (nint)_dalamudUtil.GetPetPtr((IntPtr)player));
        AddIfPresent(output, (nint)_dalamudUtil.GetMinionOrMountPtr((IntPtr)player));
        return output;
    }

    private static void AddIfPresent(HashSet<nint> output, nint address)
    {
        if (address != nint.Zero)
            output.Add(address);
    }

    private bool RemoveAnchor(nint address)
    {
        lock (_lock)
            return _activeSoundAnchors.Remove(address);
    }

    private void RebindAnchor(nint address, LocalAnimationSignature current, long now)
    {
        lock (_lock)
        {
            if (_activeSoundAnchors.TryGetValue(address, out var anchor))
            {
                anchor.Bind(current, now);
                _activeSoundAnchors[address] = anchor;
            }
        }
    }

    private void ResetAnchorMisses(nint address)
    {
        lock (_lock)
        {
            if (_activeSoundAnchors.TryGetValue(address, out var anchor) && anchor.MissedChecks != 0)
            {
                anchor.MissedChecks = 0;
                _activeSoundAnchors[address] = anchor;
            }
        }
    }

    private int RegisterAnchorMiss(nint address)
    {
        lock (_lock)
        {
            if (!_activeSoundAnchors.TryGetValue(address, out var anchor))
                return ClearAfterMissedAnimationChecks;

            anchor.MissedChecks++;
            _activeSoundAnchors[address] = anchor;
            return anchor.MissedChecks;
        }
    }

    private void RefreshPlayingSoundState()
    {
        var now = Environment.TickCount64;
        bool changed;
        lock (_lock)
        {
            changed = RefreshPlayingSoundStateUnsafe(now);
        }

        if (changed)
            PublishRefresh();
    }

    private bool RefreshPlayingSoundStateUnsafe(long now)
    {
        var shouldPlay = _activeSoundAnchors.Count != 0;
        var changed = _isPlayingSound != shouldPlay;
        _isPlayingSound = shouldPlay;
        return changed;
    }

    private bool IsLocalOwnedAddress(nint address)
    {
        if (address == nint.Zero)
            return false;

        return GetLocalOwnedAddresses().Contains(address);
    }

    private void Clear()
    {
        bool changed;
        lock (_lock)
        {
            changed = _isPlayingSound || _activeSoundAnchors.Count != 0;
            _isPlayingSound = false;
            _activeSoundAnchors.Clear();
        }

        if (changed)
            PublishRefresh();
    }

    private void PublishRefresh()
    {
        var isPlaying = IsPlayingSound;

        try
        {
            Mediator.Publish(new LocalActiveSyncIndicatorChangedMessage(isPlaying));
            Mediator.Publish(new RefreshUiMessage());
        }
        catch
        {
            // best effort; Compact UI also redraws naturally while open
        }
    }

    private LocalSoundAnimationAnchor CaptureLocalSoundAnimationAnchor(nint address, long now, TimeSpan? songLength = null)
    {
        var signature = CaptureLocalAnimationSignature(address);
        var pendingUntil = signature.IsAnchorable ? now : unchecked(now + PendingAnimationBindGraceMs);
        var expiresAt = songLength.HasValue
            ? unchecked(now + Math.Clamp((long)Math.Ceiling(songLength.Value.TotalMilliseconds), 1_000L, MaximumEstimatedSongLengthSeconds * 1000L))
            : long.MaxValue;
        return new LocalSoundAnimationAnchor(signature, pendingUntil, expiresAt);
    }

    private unsafe LocalAnimationSignature CaptureLocalAnimationSignature(nint address)
    {
        if (address == nint.Zero)
            return LocalAnimationSignature.Empty;

        try
        {
            var character = (Character*)address;
            if (character == null)
                return LocalAnimationSignature.Empty;

            var timelines = new ushort[LocalAnimationSignature.TimelineSlotCount];
            for (uint i = 0; i < LocalAnimationSignature.TimelineSlotCount; i++)
                timelines[i] = character->Timeline.TimelineSequencer.GetSlotTimeline(i);

            return new LocalAnimationSignature((byte)character->Mode, character->ModeParam, timelines);
        }
        catch
        {
            return LocalAnimationSignature.Empty;
        }
    }

    private static bool IsLiveSoundPath(string path)
        => string.Equals(Path.GetExtension(NormalizePath(path)), ".scd", StringComparison.OrdinalIgnoreCase);

    private bool TryGetSongLength(string? physicalPath, out TimeSpan songLength)
    {
        songLength = default;

        if (string.IsNullOrWhiteSpace(physicalPath))
            return false;

        try
        {
            if (!File.Exists(physicalPath))
                return false;

            var normalizedPath = Path.GetFullPath(physicalPath);
            TimeSpan? cached;
            lock (_lock)
            {
                if (_songLengthCache.TryGetValue(normalizedPath, out cached))
                {
                    if (cached.HasValue)
                    {
                        songLength = cached.Value;
                        return true;
                    }

                    return false;
                }
            }

            var fileInfo = new FileInfo(normalizedPath);
            if (fileInfo.Length < MinimumSongLikeScdBytes)
            {
                CacheSongLength(normalizedPath, null);
                return false;
            }

            var detected = TryReadEmbeddedOggDuration(normalizedPath, out var duration)
                ? ClampSongDuration(duration)
                : EstimateSongDurationFromFileSize(fileInfo.Length);

            if (detected.TotalSeconds < MinimumSongLengthSeconds)
            {
                CacheSongLength(normalizedPath, null);
                return false;
            }

            CacheSongLength(normalizedPath, detected);
            songLength = detected;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private void CacheSongLength(string path, TimeSpan? duration)
    {
        lock (_lock)
            _songLengthCache[path] = duration;
    }

    private static TimeSpan ClampSongDuration(TimeSpan duration)
    {
        if (duration.TotalSeconds < 1)
            return TimeSpan.FromSeconds(1);

        if (duration.TotalSeconds > MaximumEstimatedSongLengthSeconds)
            return TimeSpan.FromSeconds(MaximumEstimatedSongLengthSeconds);

        return duration;
    }

    private static TimeSpan EstimateSongDurationFromFileSize(long bytes)
    {
        // Most user-provided song SCDs are Vorbis-backed and sit roughly in this range. This is only
        // a fallback for unusual SCDs where embedded Ogg page parsing fails.
        var estimatedSeconds = Math.Clamp(bytes / 32_768d, MinimumSongLengthSeconds, MaximumEstimatedSongLengthSeconds);
        return TimeSpan.FromSeconds(estimatedSeconds);
    }

    private static bool TryReadEmbeddedOggDuration(string path, out TimeSpan duration)
    {
        duration = default;

        var bytes = File.ReadAllBytes(path);
        var sampleRate = ReadVorbisSampleRate(bytes);
        if (sampleRate <= 0)
            return false;

        var maxGranule = ReadMaxOggGranulePosition(bytes);
        if (maxGranule <= 0)
            return false;

        duration = TimeSpan.FromSeconds(maxGranule / (double)sampleRate);
        return duration.TotalSeconds > 0;
    }

    private static int ReadVorbisSampleRate(byte[] bytes)
    {
        for (var i = 0; i <= bytes.Length - 16; i++)
        {
            if (bytes[i] != 0x01
                || bytes[i + 1] != (byte)'v'
                || bytes[i + 2] != (byte)'o'
                || bytes[i + 3] != (byte)'r'
                || bytes[i + 4] != (byte)'b'
                || bytes[i + 5] != (byte)'i'
                || bytes[i + 6] != (byte)'s')
                continue;

            var sampleRate = BitConverter.ToInt32(bytes, i + 12);
            return sampleRate is >= 8_000 and <= 192_000 ? sampleRate : 0;
        }

        return 0;
    }

    private static long ReadMaxOggGranulePosition(byte[] bytes)
    {
        long maxGranule = 0;

        for (var i = 0; i <= bytes.Length - 14; i++)
        {
            if (bytes[i] != (byte)'O'
                || bytes[i + 1] != (byte)'g'
                || bytes[i + 2] != (byte)'g'
                || bytes[i + 3] != (byte)'S')
                continue;

            var granule = BitConverter.ToInt64(bytes, i + 6);
            if (granule > maxGranule)
                maxGranule = granule;
        }

        return maxGranule;
    }

    private static bool IsLongLivedSoundPath(string path)
    {
        var normalized = NormalizePath(path);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        return normalized.Contains("loop", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("music/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/music/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("bgm/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/bgm/", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("sound/bgm/", StringComparison.OrdinalIgnoreCase)
            || normalized.Contains("/sound/bgm/", StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizePath(string? path)
    {
        return (path ?? string.Empty)
            .Replace('\\', '/')
            .Trim()
            .TrimStart('/')
            .ToLowerInvariant();
    }

    private sealed class LocalSoundAnimationAnchor
    {
        public LocalAnimationSignature Signature { get; private set; }
        public long PendingBindUntilTick { get; private set; }
        public long ExpiresAtTick { get; }
        public int MissedChecks { get; set; }

        public LocalSoundAnimationAnchor(LocalAnimationSignature signature, long pendingBindUntilTick, long expiresAtTick)
        {
            Signature = signature;
            PendingBindUntilTick = pendingBindUntilTick;
            ExpiresAtTick = expiresAtTick;
        }

        public bool IsBound => Signature.IsAnchorable;

        public bool IsExpired(long now) => ExpiresAtTick != long.MaxValue && unchecked(ExpiresAtTick - now) <= 0;

        public void Bind(LocalAnimationSignature signature, long now)
        {
            Signature = signature;
            PendingBindUntilTick = now;
            MissedChecks = 0;
        }

        public bool IsStillActive(LocalAnimationSignature current)
        {
            if (!Signature.IsAnchorable || !current.IsAnchorable)
                return false;

            if (Signature.IsActivityMode)
                return current.Mode == Signature.Mode && (!Signature.ModeParamMatters || current.ModeParam == Signature.ModeParam);

            return Signature.ActiveTimelineSlots.Any(slot => current.IsTimelineSlotActive(slot));
        }
    }

    private readonly record struct LocalAnimationSignature(byte Mode, byte ModeParam, ushort[] TimelineIds)
    {
        public const int TimelineSlotCount = 14;
        public static readonly LocalAnimationSignature Empty = new(0, 0, Array.Empty<ushort>());

        public bool IsActivityMode => IsLocalActivityMode(Mode);
        public bool ModeParamMatters => Mode is (byte)CharacterModes.EmoteLoop or (byte)CharacterModes.InPositionLoop or (byte)CharacterModes.Performance;
        public bool HasTimelineActivity => TimelineIds.Length >= TimelineSlotCount && TimelineIds.Any(static id => id != 0);
        public bool IsAnchorable => IsActivityMode || HasTimelineActivity;
        public IEnumerable<int> ActiveTimelineSlots => Enumerable.Range(0, Math.Min(TimelineIds.Length, TimelineSlotCount)).Where(IsTimelineSlotActive);

        public bool IsTimelineSlotActive(int slot)
            => slot >= 0 && slot < TimelineIds.Length && TimelineIds[slot] != 0;

        private static bool IsLocalActivityMode(byte mode)
        {
            return mode is not 0
                and not (byte)CharacterModes.None
                and not (byte)CharacterModes.Normal
                and not (byte)CharacterModes.Dead;
        }
    }
}
