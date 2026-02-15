using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace RavaSync.Utils;

[InterpolatedStringHandler]
public readonly ref struct MareInterpolatedStringHandler
{
    readonly StringBuilder _logMessageStringbuilder;

    public MareInterpolatedStringHandler(int literalLength, int formattedCount)
    {
        _logMessageStringbuilder = new StringBuilder(literalLength);
    }

    public void AppendLiteral(string s)
    {
        _logMessageStringbuilder.Append(s);
    }

    public void AppendFormatted<T>(T t)
    {
        AppendFormatted(t, 0, null);
    }

    // Needed for $"...{value:FORMAT}..."
    public void AppendFormatted<T>(T t, string? format)
    {
        AppendFormatted(t, 0, format);
    }

    // Needed for $"...{value,ALIGN}..."
    public void AppendFormatted<T>(T t, int alignment)
    {
        AppendFormatted(t, alignment, null);
    }

    // Needed for $"...{value,ALIGN:FORMAT}..."
    public void AppendFormatted<T>(T t, int alignment, string? format)
    {
        if (t is null)
        {
            AppendAligned(string.Empty, alignment);
            return;
        }

        string text;
        if (t is IFormattable formattable)
            text = formattable.ToString(format, CultureInfo.InvariantCulture) ?? string.Empty;
        else
            text = t.ToString() ?? string.Empty;

        AppendAligned(text, alignment);
    }

    private void AppendAligned(string text, int alignment)
    {
        if (alignment == 0)
        {
            _logMessageStringbuilder.Append(text);
            return;
        }

        var width = Math.Abs(alignment);
        var pad = width - text.Length;

        if (pad <= 0)
        {
            _logMessageStringbuilder.Append(text);
            return;
        }

        if (alignment > 0)
        {
            _logMessageStringbuilder.Append(' ', pad);
            _logMessageStringbuilder.Append(text);
        }
        else
        {
            _logMessageStringbuilder.Append(text);
            _logMessageStringbuilder.Append(' ', pad);
        }
    }


    public string BuildMessage() => _logMessageStringbuilder.ToString();
}
