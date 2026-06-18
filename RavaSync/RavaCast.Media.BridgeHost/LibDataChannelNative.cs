using System.Runtime.InteropServices;
using System.Text.Json;

namespace RavaCast.Media.BridgeHost;

internal static class LibDataChannelNative
{
    private const string Library = "datachannel";
    private static bool _loggerInitialised;
    private static readonly LogCallback LogCallbackInstance = OnLog;

    public static bool TryPreload(out string detail)
    {
        try
        {
            if (!NativeLibrary.TryLoad(Library, out var handle))
            {
                detail = "Could not load datachannel native library. Expected datachannel.dll beside BridgeHost on Windows, or libdatachannel.so in the library path on Linux/Wine. For packaged builds, put the libdatachannel runtime files in RavaCast.Media.Runtime/<rid>/native so MSBuild bundles them automatically.";
                return false;
            }
            NativeLibrary.Free(handle);

            if (!_loggerInitialised)
            {
                rtcInitLogger(3, LogCallbackInstance); // RTC_LOG_WARNING
                _loggerInitialised = true;
            }

            rtcPreload();
            detail = "libdatachannel loaded and preloaded successfully.";
            return true;
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException or BadImageFormatException)
        {
            detail = "libdatachannel exists but could not be used: " + ex.Message;
            return false;
        }
        catch (Exception ex)
        {
            detail = "libdatachannel preload failed: " + ex.Message;
            return false;
        }
    }

    private static void OnLog(int level, IntPtr message)
    {
        var text = Marshal.PtrToStringAnsi(message);
        if (!string.IsNullOrWhiteSpace(text)) Program.Log("libdatachannel[" + level + "]: " + text);
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void LogCallback(int level, IntPtr message);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void DescriptionCallback(int pc, IntPtr sdp, IntPtr type, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void CandidateCallback(int pc, IntPtr cand, IntPtr mid, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void StateChangeCallback(int pc, int state, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void IceStateChangeCallback(int pc, int state, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void TrackCallback(int pc, int tr, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void DataChannelCallback(int pc, int dc, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void FrameCallback(int tr, IntPtr data, int size, IntPtr info, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void MessageCallback(int id, IntPtr data, int size, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void OpenCallback(int id, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void ClosedCallback(int id, IntPtr userPtr);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void ErrorCallback(int id, IntPtr error, IntPtr userPtr);

    [StructLayout(LayoutKind.Sequential)]
    public struct RtcConfiguration
    {
        public IntPtr iceServers;
        public int iceServersCount;
        public IntPtr proxyServer;
        public IntPtr bindAddress;
        public int certificateType;
        public int iceTransportPolicy;
        [MarshalAs(UnmanagedType.I1)] public bool enableIceTcp;
        [MarshalAs(UnmanagedType.I1)] public bool enableIceUdpMux;
        [MarshalAs(UnmanagedType.I1)] public bool disableAutoNegotiation;
        [MarshalAs(UnmanagedType.I1)] public bool forceMediaTransport;
        public ushort portRangeBegin;
        public ushort portRangeEnd;
        public int mtu;
        public int maxMessageSize;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct RtcPacketizerInit
    {
        public uint ssrc;
        public IntPtr cname;
        public byte payloadType;
        public uint clockRate;
        public ushort sequenceNumber;
        public uint timestamp;
        public ushort maxFragmentSize;
        public int nalSeparator;
        public int obuPacketization;
        public byte playoutDelayId;
        public ushort playoutDelayMin;
        public ushort playoutDelayMax;
        public byte colorSpaceId;
        public byte colorChromaSitingHorz;
        public byte colorChromaSitingVert;
        public byte colorRange;
        public byte colorPrimaries;
        public byte colorTransfer;
        public byte colorMatrix;

        public static RtcPacketizerInit ForH264(uint ssrc, byte payloadType)
            => new()
            {
                ssrc = ssrc,
                cname = Marshal.StringToHGlobalAnsi("ravacast"),
                payloadType = payloadType,
                clockRate = 90000,
                sequenceNumber = (ushort)Random.Shared.Next(0, ushort.MaxValue),
                timestamp = (uint)Random.Shared.Next(),
                maxFragmentSize = 0,
                nalSeparator = 3
            };

        public static RtcPacketizerInit ForOpus(uint ssrc, byte payloadType)
            => new()
            {
                ssrc = ssrc,
                cname = Marshal.StringToHGlobalAnsi("ravacast"),
                payloadType = payloadType,
                clockRate = 48000,
                sequenceNumber = (ushort)Random.Shared.Next(0, ushort.MaxValue),
                timestamp = (uint)Random.Shared.Next(),
                maxFragmentSize = 0
            };
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern void rtcInitLogger(int level, LogCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern void rtcPreload();

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern void rtcCleanup();

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcCreatePeerConnection(ref RtcConfiguration config);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcClosePeerConnection(int pc);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcDeletePeerConnection(int pc);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetLocalDescriptionCallback(int pc, DescriptionCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetLocalCandidateCallback(int pc, CandidateCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetStateChangeCallback(int pc, StateChangeCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetIceStateChangeCallback(int pc, IceStateChangeCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetTrackCallback(int pc, TrackCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetDataChannelCallback(int pc, DataChannelCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetLocalDescription(int pc, [MarshalAs(UnmanagedType.LPUTF8Str)] string type);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetRemoteDescription(int pc, [MarshalAs(UnmanagedType.LPUTF8Str)] string sdp, [MarshalAs(UnmanagedType.LPUTF8Str)] string type);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcAddRemoteCandidate(int pc, [MarshalAs(UnmanagedType.LPUTF8Str)] string cand, [MarshalAs(UnmanagedType.LPUTF8Str)] string? mid);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcCreateDataChannel(int pc, [MarshalAs(UnmanagedType.LPUTF8Str)] string label);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcDeleteDataChannel(int dc);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetOpenCallback(int id, OpenCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetClosedCallback(int id, ClosedCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetErrorCallback(int id, ErrorCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetMessageCallback(int id, MessageCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcAddTrack(int pc, [MarshalAs(UnmanagedType.LPUTF8Str)] string mediaDescriptionSdp);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcDeleteTrack(int tr);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetH264Packetizer(int tr, ref RtcPacketizerInit init);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetOpusPacketizer(int tr, ref RtcPacketizerInit init);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetH264Depacketizer(int tr, int nalSeparator);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetOpusDepacketizer(int tr);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSetFrameCallback(int tr, FrameCallback cb);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcSendMessage(int id, [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)] byte[] data, int size);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static extern bool rtcIsOpen(int id);


    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcGetTrackMid(int tr, [Out] byte[] buffer, int size);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcGetDataChannelLabel(int dc, [Out] byte[] buffer, int size);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcChainRtcpReceivingSession(int tr);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcChainRtcpSrReporter(int tr);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int rtcChainRtcpNackResponder(int tr, uint maxStoredPacketsCount);
}

internal sealed class RtcStringArray : IDisposable
{
    private readonly IntPtr[] _strings;
    public IntPtr Pointer { get; }
    public int Count => _strings.Length;

    private RtcStringArray(IntPtr[] strings, IntPtr pointer)
    {
        _strings = strings;
        Pointer = pointer;
    }

    public static RtcStringArray FromJson(string json)
    {
        var servers = new List<string>();
        if (!string.IsNullOrWhiteSpace(json))
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in doc.RootElement.EnumerateArray())
                    {
                        var value = item.GetString();
                        if (!string.IsNullOrWhiteSpace(value)) servers.Add(value);
                    }
                }
            }
            catch { }
        }

        if (servers.Count == 0)
            servers.Add("stun:stun.l.google.com:19302");

        var ptrs = servers.Select(Marshal.StringToHGlobalAnsi).ToArray();
        var arrayPtr = Marshal.AllocHGlobal(IntPtr.Size * ptrs.Length);
        for (var i = 0; i < ptrs.Length; i++)
            Marshal.WriteIntPtr(arrayPtr, i * IntPtr.Size, ptrs[i]);
        return new RtcStringArray(ptrs, arrayPtr);
    }

    public void Dispose()
    {
        foreach (var ptr in _strings)
            if (ptr != IntPtr.Zero) Marshal.FreeHGlobal(ptr);
        if (Pointer != IntPtr.Zero) Marshal.FreeHGlobal(Pointer);
    }
}
