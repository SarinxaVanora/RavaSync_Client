# Windows / Wine Direct Stream runtime files

Put the x64 runtime binaries in this folder before building the plugin package:

- `ffmpeg.exe`
- `datachannel.dll`
- any additional `.dll` files shipped with the same libdatachannel build

The build copies these files beside `RavaCast.Media.BridgeHost.exe`.
