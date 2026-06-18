# RavaCast Direct Stream runtime files

Direct Stream v2 uses external native/runtime media files. Keep them here so the build can bundle them automatically instead of relying on manual copying beside the renderer.

## Windows / Wine build

Place these files here:

```txt
RavaCast.Media.Runtime/win-x64/native/ffmpeg.exe
RavaCast.Media.Runtime/win-x64/native/datachannel.dll
RavaCast.Media.Runtime/win-x64/native/<any libdatachannel dependency dlls>
```

The BridgeHost project copies everything from this folder into its build/publish output. The main `RavaSync.csproj` build publishes `RavaCast.Renderer`, which publishes `RavaCast.Media.BridgeHost`, so these files end up beside:

```txt
RavaCast.Renderer.exe
RavaCast.Media.Native.dll
RavaCast.Media.BridgeHost.exe
```

The Windows/Wine publish intentionally fails with a clear MSBuild error if `ffmpeg.exe` or `datachannel.dll` is missing, because Direct Stream v2 cannot run without them.

## Linux build

Place Linux runtime files here when packaging a Linux build:

```txt
RavaCast.Media.Runtime/linux-x64/native/ffmpeg
RavaCast.Media.Runtime/linux-x64/native/libdatachannel.so
RavaCast.Media.Runtime/linux-x64/native/<any libdatachannel dependency .so files>
```

Linux builds do not currently require bundled files by default because many systems provide `ffmpeg` through PATH/package management. Files placed in this folder are still copied automatically.
