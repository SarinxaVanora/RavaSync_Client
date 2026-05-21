using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RavaSync.API.Dto.Files;

public class FilesSendDto
{
    public List<string> FileHashes { get; set; } = new();
    public List<string> UIDs { get; set; } = new();

    // RawV2 migration hint. Old clients omit these and keep legacy behaviour.
    // When ForcePayloadEncoding=true and DesiredPayloadEncoding=RawV2, the server should
    // request re-upload for already-known hashes whose stored payload is still LegacyLz4.
    public string DesiredPayloadEncoding { get; set; } = FilePayloadEncoding.LegacyLz4;
    public bool ForcePayloadEncoding { get; set; } = false;
}