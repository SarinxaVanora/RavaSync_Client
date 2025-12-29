namespace RavaSync.API.Data.Enum;

[Flags]
public enum UserPermissions
{
    NoneSet = 0,
    Paused = 1,
    DisableAnimations = 2,
    DisableSounds = 4,
    DisableVFX = 8,
    Sticky = 16,
    DisableCustomizePlus = 32,
    DisableMetaData =64,
    Disable
}