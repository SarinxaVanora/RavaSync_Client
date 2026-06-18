namespace RavaSync.Services.CharaData.Models;

public sealed record HandledCharaDataEntry(string Name, bool IsSelf, Guid? CustomizePlus, CharaDataMetaInfoExtendedDto MetaInfo, Guid? PenumbraCollection = null)
{
    public CharaDataMetaInfoExtendedDto MetaInfo { get; set; } = MetaInfo;
}
