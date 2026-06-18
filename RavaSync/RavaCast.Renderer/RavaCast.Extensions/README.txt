RavaCast WebView2 extension staging folder

WebView2 can only load unpacked browser extensions from a local folder containing manifest.json.
Bundled blocker extension folders live here using these names:

  RavaCast.Extensions\uBlockOrigin\manifest.json
  RavaCast.Extensions\Ghostery\manifest.json

Supported names are intentionally limited to uBlock/Ghostery variants so RavaCast does not
silently load unrelated or untrusted extensions from the plugin folder.

Notes:
- Do not place .crx files here; unpack them first.
- Extension files are loaded from this folder at renderer startup.
- uBlock Origin and Ghostery are loaded by default for normal browsing.
- Set RAVACAST_LOAD_UBLOCK=0 to disable uBlock Origin for a diagnostic run.
- Protected-media pages still pause bundled blockers before navigation so Amazon/Netflix-style DRM flows get stock WebView2 behaviour.
- The extension folders are copied to the plugin publish/output folder by RavaCast.Renderer.csproj.
