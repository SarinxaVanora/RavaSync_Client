namespace RavaCast.Media.BridgeHost;

/// <summary>
/// Small Annex-B access-unit splitter for FFmpeg H.264 output.
/// The FFmpeg encoder forces AUD NAL units, so each frame can be split cleanly without a full H.264 parser.
/// </summary>
internal sealed class H264AnnexBAccessUnitReader
{
    private readonly List<byte> _buffer = new(1024 * 256);

    public void Append(byte[] data, int count)
    {
        if (data.Length == 0 || count <= 0) return;
        for (var i = 0; i < count; i++) _buffer.Add(data[i]);
    }

    public bool TryTakeNext(out byte[] accessUnit)
    {
        accessUnit = [];
        var firstAud = FindNextAud(0);
        if (firstAud < 0)
        {
            TrimGarbageBeforePotentialStartCode();
            return false;
        }

        if (firstAud > 0)
            _buffer.RemoveRange(0, firstAud);

        var firstStartLen = StartCodeLengthAt(0);
        var secondAud = FindNextAud(Math.Max(1, firstStartLen + 1));
        if (secondAud < 0)
            return false;

        accessUnit = _buffer.GetRange(0, secondAud).ToArray();
        _buffer.RemoveRange(0, secondAud);
        return accessUnit.Length > 0;
    }

    private int FindNextAud(int start)
    {
        for (var i = Math.Max(0, start); i < _buffer.Count - 5; i++)
        {
            var len = StartCodeLengthAt(i);
            if (len == 0) continue;
            var nalIndex = i + len;
            if (nalIndex >= _buffer.Count) return -1;
            var nalType = _buffer[nalIndex] & 0x1F;
            if (nalType == 9) return i;
        }

        return -1;
    }

    private int StartCodeLengthAt(int index)
    {
        if (index + 3 < _buffer.Count && _buffer[index] == 0 && _buffer[index + 1] == 0 && _buffer[index + 2] == 0 && _buffer[index + 3] == 1) return 4;
        if (index + 2 < _buffer.Count && _buffer[index] == 0 && _buffer[index + 1] == 0 && _buffer[index + 2] == 1) return 3;
        return 0;
    }

    private void TrimGarbageBeforePotentialStartCode()
    {
        if (_buffer.Count <= 1024 * 1024) return;
        var keep = Math.Min(_buffer.Count, 64);
        var tail = _buffer.GetRange(_buffer.Count - keep, keep);
        _buffer.Clear();
        _buffer.AddRange(tail);
    }
}
