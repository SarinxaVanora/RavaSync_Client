# Linux Direct Stream runtime files

Put Linux runtime binaries/libraries in this folder when packaging Linux builds:

- `ffmpeg`
- `libdatachannel.so`
- any additional `.so` files shipped with the same libdatachannel build

The build copies these files beside `RavaCast.Media.BridgeHost`.
