using Microsoft.Web.WebView2.Core;
using Microsoft.Web.WebView2.WinForms;
using SharpDX;
using SharpDX.Direct3D;
using SharpDX.Direct3D11;
using SharpDX.DXGI;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Imaging;
using Point = System.Drawing.Point;
using Color = System.Drawing.Color;
using Rectangle = System.Drawing.Rectangle;
using Size = System.Drawing.Size;
using System.Globalization;
using System.IO.Pipes;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using Buffer = System.Buffer;
using Windows.Graphics;
using Windows.Graphics.Capture;
using Windows.Graphics.DirectX;
using Windows.Graphics.DirectX.Direct3D11;
using WinRT;
using D3DDevice = SharpDX.Direct3D11.Device;
using DxgiDevice = SharpDX.DXGI.Device;
using DxgiResource = SharpDX.DXGI.Resource;
using D3DTexture2D = SharpDX.Direct3D11.Texture2D;
using MapFlags = SharpDX.Direct3D11.MapFlags;
using WinFormsTimer = System.Windows.Forms.Timer;

namespace RavaCast.Renderer;

internal static class Program
{
    private const int BytesPerPixel = 4;
    private const int WM_MOUSEMOVE = 0x0200;
    private const int WM_LBUTTONDOWN = 0x0201;
    private const int WM_LBUTTONUP = 0x0202;
    private const int WM_RBUTTONDOWN = 0x0204;
    private const int WM_RBUTTONUP = 0x0205;
    private const int WM_MBUTTONDOWN = 0x0207;
    private const int WM_MBUTTONUP = 0x0208;
    private const int WM_MOUSEWHEEL = 0x020A;
    private const int WM_SETCURSOR = 0x0020;
    private const int HiddenHostParkingX = -32000;
    private const int HiddenHostParkingY = -32000;
    private const string BrowserPersonaAcceptLanguageHeader = "en-GB,en;q=0.9,en-US;q=0.8";
    private const string BrowserPersonaAcceptLanguageTags = "en-GB,en,en-US";
    private const int MK_LBUTTON = 0x0001;
    private const int MK_RBUTTON = 0x0002;
    private const int MK_MBUTTON = 0x0010;
    private static readonly Guid GraphicsCaptureItemGuid = new("79C3F95B-31F7-4EC2-A464-632EF5D30760");
    private static readonly Guid Texture2DGuid = new("6f15aaf2-d208-4e89-9ab4-489535d34f9c");
    private static readonly ConcurrentQueue<string> _pendingCommands = new();
    private static readonly ConcurrentQueue<string> _outgoingMessages = new();
    private static readonly AutoResetEvent _sendSignal = new(false);
    private static readonly object _sendLock = new();
    private static readonly object _d3dLock = new();
    private static readonly object _cursorSterileSubclassLock = new();
    private static readonly Dictionary<IntPtr, IntPtr> _cursorSterileChildWndProcs = new();
    private static readonly WndProcDelegate _cursorSterileChildWndProc = CursorSterileChildWndProc;

    private static readonly string[] BlockedHostFragments =
    [
        // Google / general ad delivery
        "doubleclick.net",
        "googlesyndication.com",
        "googleadservices.com",
        "adservice.google.",
        "googleads.",
        "pagead2.googlesyndication.com",
        "securepubads.g.doubleclick.net",
        "googleads.g.doubleclick.net",
        "imasdk.googleapis.com",
        "googletagservices.com",
        "googletagmanager.com",
        "google-analytics.com",
        "analytics.google.com",
        "firebase-settings.crashlytics.com",

        // Ad exchanges / RTB / sponsored content
        "adnxs.com",
        "adform.net",
        "adroll.com",
        "adsafeprotected.com",
        "advertising.com",
        "amazon-adsystem.com",
        "aaxads.com",
        "appnexus.com",
        "atdmt.com",
        "bidr.io",
        "bluekai.com",
        "casalemedia.com",
        "contextweb.com",
        "criteo.com",
        "criteo.net",
        "demdex.net",
        "dotomi.com",
        "exelator.com",
        "flashtalking.com",
        "lijit.com",
        "mathtag.com",
        "media.net",
        "moatads.com",
        "openx.net",
        "pubmatic.com",
        "quantserve.com",
        "rubiconproject.com",
        "scorecardresearch.com",
        "serving-sys.com",
        "sharethrough.com",
        "smartadserver.com",
        "spotxchange.com",
        "taboola.com",
        "teads.tv",
        "turn.com",
        "yieldmo.com",
        "zedo.com",
        "outbrain.com",

        // Trackers / session replay / behavioural analytics
        "adobedtm.com",
        "amplitude.com",
        "appsflyer.com",
        "branch.io",
        "braze.com",
        "clarity.ms",
        "contentsquare.net",
        "fullstory.com",
        "hotjar.com",
        "intercom.io",
        "intercomcdn.com",
        "kissmetrics.io",
        "mixpanel.com",
        "mouseflow.com",
        "newrelic.com",
        "nr-data.net",
        "optimizely.com",
        "permutive.com",
        "sentry-cdn.com",
        "segment.com",
        "segment.io",
        "segmentapis.com",
        "statsigapi.net",
        "tagcommander.com",

        // Social pixels / ad networks
        "facebook.net",
        "connect.facebook.net",
        "ads-twitter.com",
        "analytics.twitter.com",
        "bat.bing.com",
        "ct.pinterest.com",
        "snap.licdn.com",
        "tiktok.com/i18n/pixel",
        "tr.snapchat.com"
    ];

    private static readonly string[] BlockedUrlFragments =
    [
        "/ads?",
        "/ads/",
        "/ad/",
        "/adserver/",
        "/adservice/",
        "/advert/",
        "/advertisement/",
        "/analytics/",
        "/beacon/",
        "/collect?",
        "/event?",
        "/events?",
        "/gampad/",
        "/pagead/",
        "/pixel/",
        "/prebid",
        "/sponsored/",
        "/track?",
        "/tracker/",
        "/tracking/",
        "&ad_type=",
        "&adunit=",
        "&ads=",
        "&advertising_id=",
        "&client_ad=",
        "&googlead=",
        "?ad_type=",
        "?adunit=",
        "?ads=",
        "?advertising_id=",
        "?client_ad=",
        "?googlead=",
        "analytics.js",
        "fbevents.js",
        "gtag/js",
        "prebid.js"
    ];

    private static readonly string[] ProtectedMediaHostFragments =
    [
        // Services with complex DRM/auth/player pipelines. Do not run RavaCast clean-browsing
        // request blocking, cosmetic scripts, or bundled blocker extensions on these pages.
        // The goal is normal browser compatibility, not bypassing DRM or service restrictions.
        "netflix.com",
        "nflxext.com",
        "nflximg.net",
        "nflxso.net",
        "nflxvideo.net",
        "primevideo.com",
        "amazonvideo.com",
        "media-amazon.com",
        "m.media-amazon.com",
        "ssl-images-amazon.com",
        "images-amazon.com",
        "static-amazon.com",
        "atv-ext.amazon.com",
        "aiv-cdn.net",
        "amazon.",
        "amazon.co.uk",
        "amazon.com",
        "disneyplus.com",
        "disney-plus.net",
        "hulu.com",
        "max.com",
        "hbomax.com",
        "hbo.com",
        "paramountplus.com",
        "pluto.tv",
        "crunchyroll.com",
        "peacocktv.com",
        "nowtv.com",
        "youtube.com/tv",
        "tv.apple.com"
    ];

    private static readonly string[] BlockedYouTubeUrlFragments =
    [
        "/api/stats/ads",
        "/pagead/",
        "/ptracking",
        "/get_midroll_",
        "adformat=",
        "adunit=",
        "ad_preroll=",
        "ad_tag=",
        "adtype=",
        "afv_ad_tag=",
        "instream_ad",
        "invideo_ad",
        "player_ads",
        "adplacements",
        "ad_placements",
        "ad_break",
        "adbreak",
        "admodule",
        "yt_ad",
        "youtube_ad",
        "adblock"
    ];

    private const string CleanBrowsingScript = @"
(() => {
  function ravacastIsProtectedMediaPage() {
    try {
      const host = (location.hostname || '').toLowerCase();
      const href = (location.href || '').toLowerCase();
      const fragments = [
        'netflix.com', 'nflxext.com', 'nflximg.net', 'nflxso.net', 'nflxvideo.net',
        'primevideo.com', 'amazonvideo.com', 'media-amazon.com', 'aiv-cdn.net', 'amazon.co.uk', 'amazon.com',
        'disneyplus.com', 'disney-plus.net', 'hulu.com', 'max.com', 'hbomax.com', 'hbo.com',
        'paramountplus.com', 'pluto.tv', 'crunchyroll.com', 'peacocktv.com', 'nowtv.com', 'tv.apple.com'
      ];
      return fragments.some(x => host === x || host.endsWith('.' + x) || href.includes(x));
    } catch { return false; }
  }

  // Protected streaming services often treat cosmetic blockers, injected scripts and ad/tracker
  // request rewriting as player tampering. Leave those pages alone and let WebView2 behave like
  // normal Edge. This does not bypass DRM; it just stops RavaCast from breaking the page.
  if (ravacastIsProtectedMediaPage()) return;

  const styleId = 'ravacast-clean-browsing-style';
  const css = `
    .adsbygoogle,
    [id^='google_ads'],
    [id*='google_ads'],
    [id^='div-gpt-ad'],
    [id*='div-gpt-ad'],
    [id*='ad-slot' i],
    [id*='ad_container' i],
    [class*='ad-slot' i],
    [class*='adslot' i],
    [class*='advertisement' i],
    [class*='advertisment' i],
    [class*='sponsored' i],
    [class*='sponsor-banner' i],
    [aria-label='Advertisement' i],
    [data-ad],
    [data-ad-client],
    [data-ad-slot],
    [data-ad-unit],
    iframe[src*='doubleclick.net' i],
    iframe[src*='googlesyndication.com' i],
    iframe[src*='googleadservices.com' i],
    iframe[src*='amazon-adsystem.com' i],
    iframe[src*='adnxs.com' i],
    iframe[src*='taboola.com' i],
    iframe[src*='outbrain.com' i],
    ytd-display-ad-renderer,
    ytd-promoted-video-renderer,
    ytd-promoted-sparkles-web-renderer,
    ytd-promoted-sparkles-text-search-renderer,
    ytd-ad-slot-renderer,
    ytd-in-feed-ad-layout-renderer,
    ytd-banner-promo-renderer,
    ytd-statement-banner-renderer,
    ytd-search-pyv-renderer,
    ytd-rich-section-renderer:has(ytd-ad-slot-renderer),
    ytd-reel-shelf-renderer:has([href*='/shorts/']),
    .ytp-ad-module,
    .video-ads,
    .ytp-ad-player-overlay,
    .ytp-ad-overlay-container,
    .ytp-ad-text,
    .ytp-ad-preview-container,
    .ytp-paid-content-overlay,
    .ytp-ad-image-overlay,
    .ytp-ad-progress-list,
    .ytp-ad-progress,
    .ytp-ad-player-overlay-skip-or-preview,
    .ytp-ad-skip-button-container,
    .ytp-ad-skip-button-slot,
    .ytp-skip-ad-button,
    .ytp-ad-button,
    .ytp-flyout-cta,
    .ytp-ce-element,
    .ytp-suggested-action,
    .ytp-cards-teaser,
    tp-yt-paper-dialog:has(ytd-mealbar-promo-renderer),
    ytd-mealbar-promo-renderer {
      display: none !important;
      visibility: hidden !important;
      opacity: 0 !important;
      pointer-events: none !important;
      width: 0 !important;
      height: 0 !important;
      max-width: 0 !important;
      max-height: 0 !important;
    }
  `;

  function installStyle() {
    try {
      if (document.getElementById(styleId)) return;
      const style = document.createElement('style');
      style.id = styleId;
      style.textContent = css;
      (document.head || document.documentElement).appendChild(style);
    } catch { }
  }

  function normaliseText(value) {
    return (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
  }

  function clickSafeConsentDismissal() {
    try {
      const candidates = Array.from(document.querySelectorAll('button, [role=button], input[type=button], input[type=submit], a'));
      for (const el of candidates) {
        const text = normaliseText(el.innerText || el.value || el.getAttribute('aria-label') || el.textContent);
        if (!text) continue;
        if (text === 'reject all' || text === 'reject optional cookies' || text === 'continue without accepting' || text === 'decline optional cookies' || text === 'decline all') {
          el.click();
          return true;
        }
      }
    } catch { }
    return false;
  }

  function isYouTubePage() {
    try {
      const host = location.hostname.toLowerCase();
      return host === 'youtube.com' || host.endsWith('.youtube.com') || host === 'music.youtube.com' || host.endsWith('.youtube-nocookie.com');
    } catch { return false; }
  }

  function isProbablyVisible(el) {
    try {
      const rect = el.getBoundingClientRect();
      const style = getComputedStyle(el);
      return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none' && style.pointerEvents !== 'none';
    } catch { return false; }
  }

  function clickYouTubeSkipAndCloseButtons() {
    if (!isYouTubePage()) return false;
    let clicked = false;
    const selectors = [
      '.ytp-ad-skip-button',
      '.ytp-ad-skip-button-modern',
      '.ytp-skip-ad-button',
      '.ytp-ad-skip-button-container button',
      '.ytp-ad-skip-button-slot button',
      '.ytp-ad-overlay-close-button',
      '.ytp-ad-player-overlay-close-button',
      '.ytp-ad-button',
      'button[aria-label*='Skip' i]',
      'button[aria-label*='skip' i]',
      'button[title*='Skip' i]',
      'button[title*='skip' i]',
      'button:has(.ytp-ad-skip-button-icon)'
    ];

    for (const selector of selectors) {
      try {
        for (const el of Array.from(document.querySelectorAll(selector))) {
          if (!isProbablyVisible(el)) continue;
          try { el.click(); } catch { }
          try { el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); } catch { }
          clicked = true;
        }
      } catch { }
    }

    try {
      for (const el of Array.from(document.querySelectorAll('button, [role=button], .ytp-ad-skip-button-modern, .ytp-skip-ad-button'))) {
        if (!isProbablyVisible(el)) continue;
        const label = normaliseText(el.innerText || el.textContent || el.getAttribute('aria-label') || el.getAttribute('title'));
        if (label.includes('skip') || label.includes('close ad')) {
          try { el.click(); } catch { }
          clicked = true;
        }
      }
    } catch { }

    return clicked;
  }

  function isYouTubeAdShowing() {
    if (!isYouTubePage()) return false;
    try {
      const player = document.querySelector('.html5-video-player, #movie_player');
      if (player && (player.classList.contains('ad-showing') || player.classList.contains('ad-interrupting') || player.classList.contains('ad-created'))) return true;
      return !!document.querySelector('.video-ads .ad-showing, .ytp-ad-player-overlay, .ytp-ad-preview-container, .ytp-ad-text, .ytp-ad-skip-button, .ytp-skip-ad-button, .ytp-ad-skip-button-modern');
    } catch { return false; }
  }

  function pushThroughYouTubeAd() {
    if (!isYouTubeAdShowing()) return false;
    let touched = clickYouTubeSkipAndCloseButtons();
    try {
      for (const video of Array.from(document.querySelectorAll('video'))) {
        try {
          if (Number.isFinite(video.duration) && video.duration > 1 && video.currentTime < video.duration - 0.5) {
            video.currentTime = Math.max(video.currentTime, video.duration - 0.25);
            touched = true;
          }
          video.playbackRate = 16;
          video.muted = true;
          if (video.paused) video.play().catch(() => {});
        } catch { }
      }
    } catch { }
    return touched;
  }

  function scrubYouTubeAdFields(obj, depth) {
    if (!obj || typeof obj !== 'object' || depth > 28) return false;
    let changed = false;
    const keys = [
      'adPlacements', 'adSlots', 'playerAds', 'playerLegacyDesktopWatchAdsRenderer',
      'adBreakHeartbeatParams', 'adParams', 'adSafetyReason', 'adSignals',
      'adTrackingParams', 'adTargeting', 'adTagParameters', 'ad3_module',
      'promotedSparklesWebRenderer', 'promotedSparklesTextSearchRenderer',
      'searchPyvRenderer', 'displayAdRenderer', 'statementBannerRenderer',
      'mealbarPromoRenderer', 'primetimePromoRenderer'
    ];

    for (const key of keys) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        try { delete obj[key]; changed = true; } catch { }
      }
    }

    try {
      if (obj.playerConfig && obj.playerConfig.args) {
        for (const key of Object.keys(obj.playerConfig.args)) {
          const lower = key.toLowerCase();
          if (lower.includes('ad') || lower.includes('afv')) {
            delete obj.playerConfig.args[key];
            changed = true;
          }
        }
      }
    } catch { }

    try {
      const values = Array.isArray(obj) ? obj : Object.values(obj);
      for (const value of values) {
        if (value && typeof value === 'object') {
          changed = scrubYouTubeAdFields(value, depth + 1) || changed;
        }
      }
    } catch { }

    return changed;
  }

  function scrubYouTubeJsonText(text) {
    try {
      if (!text || text.length < 2) return text;
      const data = JSON.parse(text);
      return scrubYouTubeAdFields(data, 0) ? JSON.stringify(data) : text;
    } catch {
      return text;
    }
  }

  function isYouTubePlayerApiUrl(value) {
    try {
      const raw = typeof value === 'string' ? value : (value && value.url) ? value.url : '';
      if (!raw) return false;
      const url = new URL(raw, location.href);
      const host = url.hostname.toLowerCase();
      if (!(host === 'youtube.com' || host.endsWith('.youtube.com') || host.endsWith('.youtube-nocookie.com'))) return false;
      return url.pathname.includes('/youtubei/v1/player') || url.pathname.includes('/get_video_info') || url.pathname.includes('/watch_next');
    } catch { return false; }
  }

  function installYouTubeFetchScrubber() {
    if (!isYouTubePage() || window.__ravacastYouTubeFetchScrubberInstalled) return;
    window.__ravacastYouTubeFetchScrubberInstalled = true;

    try {
      const nativeFetch = window.fetch;
      if (typeof nativeFetch === 'function') {
        window.fetch = async function(...args) {
          const response = await nativeFetch.apply(this, args);
          try {
            if (!isYouTubePlayerApiUrl(args[0])) return response;
            const clone = response.clone();
            const text = await clone.text();
            const cleaned = scrubYouTubeJsonText(text);
            if (cleaned === text) return response;
            const headers = new Headers(response.headers);
            headers.delete('content-length');
            return new Response(cleaned, { status: response.status, statusText: response.statusText, headers });
          } catch {
            return response;
          }
        };
      }
    } catch { }
  }

  function scrubYouTubeGlobals() {
    if (!isYouTubePage()) return;
    try {
      if (window.ytInitialPlayerResponse) scrubYouTubeAdFields(window.ytInitialPlayerResponse, 0);
      if (window.ytplayer && window.ytplayer.config) scrubYouTubeAdFields(window.ytplayer.config, 0);
      if (window.ytcfg && typeof window.ytcfg.get === 'function') {
        const player = window.ytcfg.get('PLAYER_CONFIG');
        if (player) scrubYouTubeAdFields(player, 0);
      }
    } catch { }
  }

  function tidyYouTubePlayer() {
    if (!isYouTubePage()) return;
    installYouTubeFetchScrubber();
    scrubYouTubeGlobals();
    clickYouTubeSkipAndCloseButtons();
    pushThroughYouTubeAd();
  }

  let scheduled = false;
  function tidy() {
    scheduled = false;
    installStyle();
    clickSafeConsentDismissal();
    tidyYouTubePlayer();
  }

  function scheduleTidy() {
    if (scheduled) return;
    scheduled = true;
    setTimeout(tidy, 250);
  }

  tidy();
  try { setInterval(tidyYouTubePlayer, 250); } catch { }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', tidy, { once: true });
  }

  try {
    const observer = new MutationObserver(scheduleTidy);
    observer.observe(document.documentElement, { childList: true, subtree: true });
  } catch { }
})();";

    private static BrowserHostForm? _form;
    private static bool _interactiveBrowserWindowVisible;
    private static WebView2? _webView;
    private static CoreWebView2? _core;
    private static StreamReader? _reader;
    private static StreamWriter? _writer;
    private static D3DDevice? _d3dDevice;
    private static DeviceContext? _d3dContext;
    private static D3DTexture2D? _sharedTexture;
    private static IDirect3DDevice? _winRtDevice;
    private static GraphicsCaptureItem? _captureItem;
    private static Direct3D11CaptureFramePool? _framePool;
    private static GraphicsCaptureSession? _captureSession;
    private static WinFormsTimer? _pumpTimer;
    private static WinFormsTimer? _zOrderTimer;
    private static WinFormsTimer? _cookieTimer;
    private static WinFormsTimer? _mediaStateTimer;
    private static readonly object _cleanBrowsingExtensionGate = new();
    private static readonly List<CoreWebView2BrowserExtension> _cleanBrowsingExtensions = [];
    private static bool _protectedMediaModeActive;
    private static bool _ravaCastBrowserExtensionsInstalled;
    private static bool _ravaCastBrowserChromeInstalled;
    private static bool _cleanBrowsingModeInstalled;
    private static string? _desktopEdgeUserAgent;
    private static string? _desktopEdgeMajorVersion;
    private static bool _desktopEdgePersonaRequestShimInstalled;
    private static bool _desktopEdgePersonaScriptInstalled;
    private static bool _primeWebView2ClientHintSanitizerInstalled;
    private static bool _primeAmazonBrowserPersonaInstalled;
    private static bool _primeAmazonBrowserPersonaRequestShimInstalled;
    private static bool _streamingCookieTrackingPreventionDisabled;
    private static string? _primeAmazonBrowserPersonaUserAgent;
    private static string? _primeAmazonBrowserPersonaFullVersion;
    private static string? _primeAmazonBrowserPersonaMajorVersion;
    private static long _lastPrimeBrowserPersonaRequestLogTick;
    private static long _lastPrimeBrowserPersonaErrorTick;
    private static long _lastPrimeClientHintSanitizerErrorTick;
    private static long _lastPrimeClientHintSanitizerLogTick;
    private static bool _webMessagesEnabled;
    private static bool _protectedMediaDiagnosticsInstalled;
    private static long _lastDesktopEdgePersonaHeaderErrorTick;
    private static long _lastProtectedMediaDiagnosticTick;
    private static int _primeResourceDiagnosticCount;
    private static readonly ConcurrentDictionary<string, PrimeResourceRequestDiagnostic> _primeResourceDiagnosticsByRequestId = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, byte> _primeResponseBodyLoggedByRequestId = new(StringComparer.OrdinalIgnoreCase);
    private static string? _lastNavigateRequestUrl;
    private static long _lastNavigateRequestTick;
    private static long _lastProtectedMediaModeSwitchTick;
    private static BrowserCaptureMethod _browserCaptureMethod = BrowserCaptureMethod.GdiWindow;
    private static GdiWindowCaptureMethod _gdiWindowCaptureMethod = GdiWindowCaptureMethod.Auto;
    private static WinFormsTimer? _gdiCaptureTimer;
    private static CancellationTokenSource? _gdiCaptureLoopCts;
    private static Task? _gdiCaptureLoopTask;
    private static readonly object _gdiCaptureSync = new();
    private static Bitmap? _gdiCaptureBitmap;
    private static IntPtr _gdiCaptureHwnd;
    private static long _lastGdiTargetProbeTick;
    private static bool _sentFirstGdiFrame;
    private static int _trustedNonBlankGdiFrames;
    private static long _lastTrustedNonBlankGdiFrameTick;
    private static long _lastGdiCaptureLoopWarningTick;
    private static long _lastGdiWarningTick;
    private static long _lastGdiBlankWarningTick;
    private static long _lastGdiAutoFallbackTick;
    private static int _consecutiveBlankGdiFrames;
    private static bool _usingCapturePreviewFallback;
    private static bool _capturePreviewInFlight;
    private static long _lastCapturePreviewErrorTick;
    private static WinFormsTimer? _capturePreviewTimer;

    private static int _width = 1280;
    private static int _height = 720;
    private static int _captureWidth = 1280;
    private static int _captureHeight = 720;
    private static int _browserViewportWidth = 1280;
    private static int _browserViewportHeight = 720;
    private static double _browserDevicePixelRatio = 1.0;
    private static long _lastViewportMetricRefreshTick;
    private static float _volume = 0.5f;
    private static bool _muted;
    private static int _parentPid;
    private static string _profilePath = string.Empty;
    private static string? _rendererLogPath;
    private static readonly object _rendererLogLock = new();
    private static string? _currentUrl;
    private static volatile bool _shutdownRequested;
    private static bool _directStreamReceiverOnly;
    private static long? _preferredAdapterLuid;
    private static bool _webViewReady;
    private static bool _captureReady;
    private static bool _sentFirstFrame;
    private static long _frameIndex;
    private static long _lastFrameMessageTick;
    private static IntPtr _sharedHandle;
    private static bool _directStreamTextureSizeLockNoticeSent;
    private static int _sharedTextureWidth = 1280;
    private static int _sharedTextureHeight = 720;
    private static IntPtr _gameWindow;
    private static bool _consentCookiesDirty;
    private static long _lastConsentCookieExportTick;
    private static SharedConsentCookie[] _pendingImportConsentCookies = [];
    private static bool _pendingImportConsentReload;
    private static double? _pendingMediaSyncPosition;
    private static bool _pendingMediaSyncPlaying = true;
    private static long _pendingMediaSyncUntilTick;
    private static long _lastMediaStateSentTick;

    private enum BrowserCaptureMethod
    {
        GdiWindow,
        Wgc,
        CapturePreview
    }

    private enum GdiWindowCaptureMethod
    {
        Auto,
        BitBlt,
        PrintWindow
    }

    private sealed class PrimeResourceRequestDiagnostic
    {
        public string Url { get; init; } = string.Empty;
        public string Type { get; init; } = string.Empty;
        public string DocumentUrl { get; init; } = string.Empty;
        public string Initiator { get; init; } = string.Empty;
    }

    [STAThread]
    private static int Main(string[] args)
    {
        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;

        var parsed = ParseArgs(args);
        _browserCaptureMethod = ResolveBrowserCaptureMethod();
        _gdiWindowCaptureMethod = ResolveGdiWindowCaptureMethod();
        if (!parsed.TryGetValue("pipe", out var pipeName) || string.IsNullOrWhiteSpace(pipeName)) return 2;
        if (parsed.TryGetValue("width", out var w) && int.TryParse(w, NumberStyles.Integer, CultureInfo.InvariantCulture, out var wi)) _width = Math.Clamp(wi, 320, 3840);
        if (parsed.TryGetValue("height", out var h) && int.TryParse(h, NumberStyles.Integer, CultureInfo.InvariantCulture, out var he)) _height = Math.Clamp(he, 180, 2160);
        _captureWidth = _width;
        _captureHeight = _height;
        _browserViewportWidth = _width;
        _browserViewportHeight = _height;
        if (parsed.TryGetValue("parent", out var parent) && int.TryParse(parent, NumberStyles.Integer, CultureInfo.InvariantCulture, out var pid)) _parentPid = pid;
        if (parsed.TryGetValue("adapter-luid", out var adapterLuidText) && long.TryParse(adapterLuidText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var adapterLuid))
            _preferredAdapterLuid = adapterLuid;
        else if (long.TryParse(Environment.GetEnvironmentVariable("RAVACAST_D3D_ADAPTER_LUID"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var envAdapterLuid))
            _preferredAdapterLuid = envAdapterLuid;
        _directStreamReceiverOnly = parsed.ContainsKey("direct-stream-receiver-only");
        _profilePath = parsed.TryGetValue("profile", out var profile) && !string.IsNullOrWhiteSpace(profile)
            ? profile
            : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "RavaSync", "RavaCast", "WebView2Profile");
        _rendererLogPath = parsed.TryGetValue("log", out var logPath) && !string.IsNullOrWhiteSpace(logPath)
            ? logPath
            : Path.Combine(_profilePath, "RavaCast.Renderer.log");
        LogRenderer("Renderer process starting. Args=" + string.Join(" ", args.Select(a => a.Contains(' ') ? "\"" + a + "\"" : a)));
        LogRenderer($"Renderer capture mode selected: {_browserCaptureMethod}; GDI method: {_gdiWindowCaptureMethod}");

        try
        {
            Directory.CreateDirectory(_profilePath);
            InitialiseD3D();

            using var pipe = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
            pipe.Connect(10000);
            _reader = new StreamReader(pipe);
            _writer = new StreamWriter(pipe) { AutoFlush = true };

            var sendThread = new Thread(SendLoop) { IsBackground = true, Name = "RavaCast WebView2 IPC Send" };
            sendThread.Start();
            var commandThread = new Thread(CommandLoop) { IsBackground = true, Name = "RavaCast WebView2 IPC" };
            commandThread.Start();
            StartParentWatch(_parentPid);

            try { Application.SetHighDpiMode(HighDpiMode.PerMonitorV2); } catch { }
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            // Do not install a process-wide cursor message filter. The hidden/no-activate
            // renderer window must not force the system cursor while the game owns input; doing
            // so can make the hardware cursor blink/flicker even after RavaCast is closed.

            _form = BuildHostForm();
            if (_directStreamReceiverOnly)
            {
                _form.Text = "RavaCast Direct Stream Receiver Host";
                _form.Shown += (_, _) => Send(new { op = "status", text = "Direct Stream receiver host ready", detail = "Receiver mode is running without attaching a WebView2 browser surface." });
            }
            else
            {
                _webView = BuildWebView(_form);
                _webView.SizeChanged += (_, _) => RefreshBrowserViewportMetrics();
                _form.Controls.Add(_webView);
                _form.Shown += async (_, _) => await InitialiseWebViewAndCaptureAsync().ConfigureAwait(true);
            }
            _form.FormClosed += (_, _) => _shutdownRequested = true;

            _pumpTimer = new WinFormsTimer { Interval = 5 };
            _pumpTimer.Tick += (_, _) => PumpPendingCommands();
            _pumpTimer.Start();

            if (!_directStreamReceiverOnly)
            {
                _zOrderTimer = new WinFormsTimer { Interval = 2000 };
                _zOrderTimer.Tick += (_, _) => KeepHostBehindGame();
                _zOrderTimer.Start();

                _cookieTimer = new WinFormsTimer { Interval = 1500 };
                _cookieTimer.Tick += async (_, _) => await ExportConsentCookiesIfDirtyAsync().ConfigureAwait(true);
                _cookieTimer.Start();

                _mediaStateTimer = new WinFormsTimer { Interval = 1000 };
                _mediaStateTimer.Tick += (_, _) => PollMediaState();
                _mediaStateTimer.Start();
            }

            Send(new { op = "status", text = _directStreamReceiverOnly ? "Direct Stream receiver host starting; browser UI disabled" : "WebView2 host starting; waiting for Edge runtime" });
            Send(new { op = "directStreamStatus", text = DirectStreamBridge.InitialStatusText, detail = DirectStreamBridge.InitialStatusDetail, publisherActive = false, receiverActive = false, nativeMediaAvailable = DirectStreamBridge.NativeMediaAvailable });

            Application.Run(_form);
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 renderer fatal: " + ex });
            return 10;
        }
        finally
        {
            _shutdownRequested = true;
            try { _sendSignal.Set(); } catch { }
            DirectStreamBridge.StopAll("Renderer shutting down");
            ReleaseCursorSterileChildSubclasses();
            ShutdownCapture();
            ShutdownD3D();
            try { _pumpTimer?.Dispose(); } catch { }
            try { _zOrderTimer?.Dispose(); } catch { }
            try { _cookieTimer?.Dispose(); } catch { }
            try { _mediaStateTimer?.Dispose(); } catch { }
            try { _webView?.Dispose(); } catch { }
            try { _form?.Dispose(); } catch { }
        }

        return 0;
    }

    private static BrowserHostForm BuildHostForm()
    {
        var form = new BrowserHostForm
        {
            Text = "RavaCast Browser Host",
            FormBorderStyle = FormBorderStyle.None,
            ShowInTaskbar = false,
            StartPosition = FormStartPosition.Manual,
            Size = new Size(_width, _height),
            ClientSize = new Size(_width, _height),
            BackColor = Color.Black,
            Opacity = 1.0,
            TopMost = false
        };

        var gameRect = TryGetGameRect(out var rect) ? rect : Screen.PrimaryScreen?.Bounds ?? new Rectangle(0, 0, _width, _height);
        form.Location = new Point(gameRect.Left + 16, gameRect.Top + 16);
        return form;
    }

    private static WebView2 BuildWebView(Form form)
        => new()
        {
            Dock = DockStyle.Fill,
            DefaultBackgroundColor = UseBrowserDarkMode() ? Color.Black : Color.White,
            AllowExternalDrop = false,
            CreationProperties = null,
            Size = form.ClientSize
        };

    private static string BuildWebView2BrowserArguments()
    {
        var args = new List<string>();

        // Compatibility capture deliberately forces a software-friendly rendering path for GDI/BitBlt capture.
        // This is not an exact clone of Chrome's "Use graphics acceleration when available" toggle; it is a
        // stronger WebView2/Chromium process flag path that keeps accelerated swap-chain surfaces out of capture.
        var forceHardwareAcceleration = IsEnvEnabled("RAVACAST_WEBVIEW2_HARDWARE_ACCELERATION");
        var forceDisableGpu = IsEnvEnabled("RAVACAST_WEBVIEW2_DISABLE_GPU")
            || IsEnvEnabled("RAVACAST_WEBVIEW2_DISABLE_HARDWARE_ACCELERATION")
            || IsEnvEnabled("RAVACAST_WEBVIEW2_SOFTWARE_RENDER");

        if ((_browserCaptureMethod == BrowserCaptureMethod.GdiWindow || forceDisableGpu) && !forceHardwareAcceleration)
            args.Add("--disable-gpu");

        if (UseBrowserDarkMode())
        {
            args.Add("--force-dark-mode");
            args.Add("--enable-features=WebContentsForceDark");
        }

        // Keep the old frame-capture helper on by default. This avoids Chromium using accelerated video decode
        // surfaces even when the compositor is otherwise behaving. It can be explicitly disabled for A/B testing.
        var acceleratedVideoDecode = Environment.GetEnvironmentVariable("RAVACAST_WEBVIEW2_ACCELERATED_VIDEO_DECODE");
        var disableAcceleratedVideoDecode = Environment.GetEnvironmentVariable("RAVACAST_WEBVIEW2_DISABLE_ACCELERATED_VIDEO_DECODE");
        var disableVideoDecode = !string.Equals(acceleratedVideoDecode, "1", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(disableAcceleratedVideoDecode, "0", StringComparison.OrdinalIgnoreCase);
        if (disableVideoDecode)
            args.Add("--disable-accelerated-video-decode");

        var extra = Environment.GetEnvironmentVariable("RAVACAST_WEBVIEW2_EXTRA_ARGS");
        if (!string.IsNullOrWhiteSpace(extra))
            args.Add(extra.Trim());

        return string.Join(" ", args.Where(a => !string.IsNullOrWhiteSpace(a)));
    }

    private static BrowserCaptureMethod ResolveBrowserCaptureMethod()
    {
        var raw = Environment.GetEnvironmentVariable("RAVACAST_BROWSER_CAPTURE_METHOD")
            ?? Environment.GetEnvironmentVariable("RAVACAST_WEBVIEW2_CAPTURE_METHOD")
            ?? Environment.GetEnvironmentVariable("RAVACAST_CAPTURE_METHOD");

        if (string.Equals(raw, "wgc", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "windowsgraphicscapture", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "windows-graphics-capture", StringComparison.OrdinalIgnoreCase))
            return BrowserCaptureMethod.Wgc;

        if (string.Equals(raw, "capturepreview", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "capture-preview", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "webview", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "screenshot", StringComparison.OrdinalIgnoreCase))
            return BrowserCaptureMethod.CapturePreview;

        return BrowserCaptureMethod.GdiWindow;
    }

    private static GdiWindowCaptureMethod ResolveGdiWindowCaptureMethod()
    {
        var raw = Environment.GetEnvironmentVariable("RAVACAST_GDI_CAPTURE_METHOD")
            ?? Environment.GetEnvironmentVariable("RAVACAST_WINDOW_CAPTURE_METHOD");

        if (string.Equals(raw, "bitblt", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "blt", StringComparison.OrdinalIgnoreCase))
            return GdiWindowCaptureMethod.BitBlt;
        if (string.Equals(raw, "printwindow", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "print", StringComparison.OrdinalIgnoreCase))
            return GdiWindowCaptureMethod.PrintWindow;

        return GdiWindowCaptureMethod.Auto;
    }

    private static bool IsEnvEnabled(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "on", StringComparison.OrdinalIgnoreCase);
    }

    private static bool UseBrowserDarkMode()
    {
        var value = Environment.GetEnvironmentVariable("RAVACAST_WEBVIEW2_DARK_MODE");
        return !string.Equals(value, "0", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(value, "no", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(value, "off", StringComparison.OrdinalIgnoreCase);
    }

    private static bool KeepHiddenHostNearGameWindow()
        => IsEnvEnabled("RAVACAST_KEEP_HIDDEN_HOST_NEAR_GAME");

    private static bool UseProtectedMediaCompatibilityMode()
        => IsEnvEnabled("RAVACAST_ENABLE_PROTECTED_MEDIA_MODE");

    private static bool UseDesktopEdgeUserAgentOverride()
        => IsEnvEnabled("RAVACAST_ENABLE_DESKTOP_EDGE_UA")
            || IsEnvEnabled("RAVACAST_WEBVIEW2_FULL_DESKTOP_EDGE_PERSONA");

    private static bool UsePrimeClientHintSanitizer()
        => IsEnvEnabled("RAVACAST_ENABLE_PRIME_WEBVIEW2_CH_SANITIZER");

    private static bool UsePrimeAmazonBrowserPersona()
        => !IsEnvEnabled("RAVACAST_DISABLE_BROWSER_PERSONA")
            && !IsEnvEnabled("RAVACAST_DISABLE_PRIME_AMAZON_BROWSER_PERSONA");

    private static bool UseStreamingCookieCompatibilityMode()
        => !IsEnvEnabled("RAVACAST_DISABLE_STREAMING_COOKIE_COMPATIBILITY")
            && !IsEnvEnabled("RAVACAST_WEBVIEW2_KEEP_TRACKING_PREVENTION");

    private static Task EnsureRavaCastBrowserChromeInstalledAsync()
    {
        // Navigation now lives in the plugin Current Cast UI. Keep the captured WebView surface
        // clean so fullscreen video/page content is not covered by injected browser chrome.
        _ravaCastBrowserChromeInstalled = true;
        return Task.CompletedTask;
    }

    private static Task ForceRavaCastBrowserChromeAsync()
        => Task.CompletedTask;

    private static void TryApplyBrowserPreferredColorScheme()
    {
        try
        {
            if (_core?.Profile is null) return;

            // Keep this reflection-based so older bundled WebView2 assemblies still compile/run.
            var property = _core.Profile.GetType().GetProperty("PreferredColorScheme");
            if (property?.CanWrite != true) return;

            var value = Enum.Parse(property.PropertyType, UseBrowserDarkMode() ? "Dark" : "Auto");
            property.SetValue(_core.Profile, value);
            Send(new { op = "status", text = UseBrowserDarkMode() ? "WebView2 browser dark mode enabled" : "WebView2 browser dark mode disabled" });
        }
        catch { }
    }

    private static async Task InitialiseWebViewAndCaptureAsync()
    {
        if (_form is null || _webView is null) return;
        try
        {
            KeepHostBehindGame();

            var browserArgs = BuildWebView2BrowserArguments();
            var options = new CoreWebView2EnvironmentOptions(browserArgs);
            options.AreBrowserExtensionsEnabled = true;
            var environment = await CoreWebView2Environment.CreateAsync(null, _profilePath, options).ConfigureAwait(true);
            Send(new { op = "status", text = "WebView2 Evergreen runtime selected: " + environment.BrowserVersionString });
            Send(new { op = "status", text = string.IsNullOrWhiteSpace(browserArgs) ? "WebView2 browser args: stock defaults" : "WebView2 browser args active: " + browserArgs });
            Send(new { op = "status", text = _browserCaptureMethod == BrowserCaptureMethod.GdiWindow
                ? $"WebView2 capture mode: OBS-style GDI window capture ({_gdiWindowCaptureMethod})"
                : _browserCaptureMethod == BrowserCaptureMethod.CapturePreview
                    ? "WebView2 capture mode: WebView2 preview fallback"
                    : "WebView2 capture mode: Windows Graphics Capture" });
            await _webView.EnsureCoreWebView2Async(environment).ConfigureAwait(true);
            _core = _webView.CoreWebView2;
            TryApplyBrowserPreferredColorScheme();

            try { _core.Settings.AreDefaultContextMenusEnabled = true; } catch { }
            try { _core.Settings.AreDevToolsEnabled = true; } catch { }
            try { _core.Settings.IsScriptEnabled = true; } catch { }
            _webMessagesEnabled = IsEnvEnabled("RAVACAST_WEBVIEW2_ENABLE_WEB_MESSAGES");
            try { _core.Settings.IsWebMessageEnabled = _webMessagesEnabled; } catch { }
            try { _core.Settings.AreHostObjectsAllowed = false; } catch { }
            try { _core.Settings.AreBrowserAcceleratorKeysEnabled = true; } catch { }
            Send(new { op = "status", text = _webMessagesEnabled
                ? "WebView2 host messaging enabled by environment for diagnostics"
                : "WebView2 host messaging disabled so public pages do not see chrome.webview" });

            if (UseStreamingCookieCompatibilityMode())
                TryRelaxStreamingCookieProfileSettings();
            else
                Send(new { op = "status", text = "Streaming cookie compatibility disabled by environment; WebView2 tracking prevention left unchanged" });

            if (UseDesktopEdgeUserAgentOverride())
                await ApplyDesktopEdgePersonaAsync(environment.BrowserVersionString).ConfigureAwait(true);
            else
                Send(new { op = "status", text = "WebView2 desktop Edge user-agent override disabled; using stock WebView2 user-agent" });

            if (UsePrimeAmazonBrowserPersona())
                await ApplyPrimeAmazonBrowserPersonaAsync(environment.BrowserVersionString).ConfigureAwait(true);
            else
                Send(new { op = "status", text = "Global browser persona disabled by environment; WebView2 client hints may expose Microsoft Edge WebView2" });

            if (UsePrimeClientHintSanitizer())
                InstallPrimeWebView2ClientHintSanitizer();
            else
                Send(new { op = "status", text = "Prime/Amazon legacy client-hint sanitizer disabled; unified browser persona handles UA/client hints" });

            await InstallProtectedMediaDiagnosticsAsync().ConfigureAwait(true);
            await EnsureRavaCastBrowserChromeInstalledAsync().ConfigureAwait(true);

            if (UseProtectedMediaCompatibilityMode())
                TryRelaxProtectedMediaProfileSettings();
            else
                Send(new { op = "status", text = "Protected media compatibility mode disabled; Prime/Amazon uses stock WebView2 + bundled extensions" });
            try { _core.PermissionRequested += OnPermissionRequested; } catch { }
            try
            {
                _core.NavigationStarting += (_, e) =>
                {
                    Send(new { op = "status", text = "WebView2 navigation starting: " + e.Uri });
                    ApplyProtectedMediaModeForUrl(e.Uri);
                };
                _core.ContentLoading += (_, e) => Send(new { op = "status", text = "WebView2 content loading", navigationId = e.NavigationId });
                _core.DOMContentLoaded += (_, e) => Send(new { op = "status", text = "WebView2 DOM content loaded", navigationId = e.NavigationId });
                _core.HistoryChanged += (_, _) =>
                {
                    try { Send(new { op = "status", text = "WebView2 history changed: " + _core.Source }); }
                    catch { }
                };
                if (_webMessagesEnabled)
                    _core.WebMessageReceived += OnWebMessageReceived;
            }
            catch { }

            var startingOnProtectedMedia = IsProtectedMediaUrl(_currentUrl);
            if (startingOnProtectedMedia && UseProtectedMediaCompatibilityMode())
            {
                _protectedMediaModeActive = true;
                Send(new { op = "status", text = "Protected media compatibility mode pre-armed before extension/clean-browsing install" });
                Send(new { op = "status", text = "Protected media pristine startup: bundled blockers and clean-browsing scripts will not be installed before this navigation" });
            }
            else if (startingOnProtectedMedia)
            {
                if (!_ravaCastBrowserExtensionsInstalled)
                    await InstallRavaCastBrowserExtensionsAsync().ConfigureAwait(true);
                Send(new { op = "status", text = "Prime/Amazon browser mode active: extensions loaded, no protected-mode toggles, no clean-browsing document script; global WebView2 UA client hints masked" });
            }
            else
            {
                // Do not mark the browser as ready until extensions and document-start filters
                // are installed for normal sites. Otherwise the first requested page can navigate
                // before uBlock/Ghostery/clean-browsing scripts are active, so blockers only start
                // working on the second navigation.
                await EnsureNormalBrowsingToolsInstalledAsync().ConfigureAwait(true);
            }
            _webViewReady = true;

            try { _core.NewWindowRequested += OnNewWindowRequested; } catch { }

            _core.SourceChanged += (_, _) =>
            {
                try
                {
                    var source = _core.Source;
                    if (!string.IsNullOrWhiteSpace(source) && !source.StartsWith("about:", StringComparison.OrdinalIgnoreCase))
                    {
                        ApplyProtectedMediaModeForUrl(source);
                        Send(new { op = "urlChanged", url = source });
                        _ = ForceRavaCastBrowserChromeAsync();
                        MarkConsentCookiesDirty();
                    }
                }
                catch { }
            };

            _core.NavigationCompleted += (_, e) =>
            {
                if (e.IsSuccess)
                {
                    Send(new { op = "status", text = "WebView2 page loaded" });
                    _ = ForceRavaCastBrowserChromeAsync();
                    RefreshBrowserViewportMetrics();
                    ApplyAudioState();
                    ApplyPendingMediaSync(force: true);
                    MarkConsentCookiesDirty();
                    if (_core is not null && IsProtectedMediaUrl(_core.Source))
                    {
                        _ = ProbeProtectedMediaSupportAsync(_core.Source);
                        _ = LogPrimeCookieJarSnapshotAsync(_core.Source);
                    }
                }
                else
                {
                    Send(new { op = "error", message = $"WebView2 navigation failed: {e.WebErrorStatus}" });
                }
            };
            _core.ProcessFailed += (_, e) => Send(new { op = "error", message = $"WebView2 process failed: {e.ProcessFailedKind}" });
            _core.ContainsFullScreenElementChanged += (_, _) =>
            {
                if (_core.ContainsFullScreenElement)
                    Send(new { op = "status", text = "WebView2 fullscreen content is active in the RavaCast browser host" });
            };

            StartBrowserCapture();
            RefreshBrowserViewportMetrics();
            Send(new { op = "ready", width = _width, height = _height });
            Send(new { op = "status", text = "WebView2 renderer ready" });

            await ApplyPendingConsentCookiesAsync().ConfigureAwait(true);
            if (!string.IsNullOrWhiteSpace(_currentUrl))
                await NavigateCoreAsync(_currentUrl).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 initialisation failed: " + ex });
        }
    }

    private static void InitialiseD3D()
    {
        _d3dDevice = CreateRendererD3DDevice();
        _d3dContext = _d3dDevice.ImmediateContext;
        RecreateSharedTextureLocked(_width, _height);

        using var dxgiDevice = _d3dDevice.QueryInterface<DxgiDevice>();
        var hr = CreateDirect3D11DeviceFromDXGIDevice(dxgiDevice.NativePointer, out var inspectable);
        if (hr < 0) Marshal.ThrowExceptionForHR(hr);
        try
        {
            _winRtDevice = WinRT.MarshalInterface<IDirect3DDevice>.FromAbi(inspectable);
        }
        finally
        {
            Marshal.Release(inspectable);
        }
    }

    private static D3DDevice CreateRendererD3DDevice()
    {
        var flags = DeviceCreationFlags.BgraSupport;
        var featureLevels = new[] { FeatureLevel.Level_11_1, FeatureLevel.Level_11_0 };

        if (_preferredAdapterLuid is long preferredLuid && preferredLuid != 0)
        {
            try
            {
                using var factory = new Factory1();
                foreach (var adapter in factory.Adapters1)
                {
                    try
                    {
                        if (adapter.Description1.Luid != preferredLuid) continue;
                        LogRenderer($"Using game D3D adapter for RavaCast shared textures; LUID={preferredLuid}.");
                        return new D3DDevice(adapter, flags, featureLevels);
                    }
                    finally
                    {
                        try { adapter.Dispose(); } catch { }
                    }
                }

                LogRenderer($"Requested RavaCast D3D adapter LUID {preferredLuid} was not found; falling back to default hardware adapter.");
            }
            catch (Exception ex)
            {
                LogRenderer($"Failed to create RavaCast D3D device on requested adapter LUID {preferredLuid}: {ex.Message}; falling back to default hardware adapter.");
            }
        }

        return new D3DDevice(DriverType.Hardware, flags, featureLevels);
    }


    private static IntPtr GetTexturePointerFromDirect3DSurface(IDirect3DSurface surface)
    {
        if (surface is not IWinRTObject winRtSurface)
            throw new InvalidOperationException("WGC frame surface did not expose a WinRT native object.");

        // C#/WinRT exposes WinRT/COM interop through IWinRTObject.NativeObject.
        // Avoid IDirect3DSurface.As<T>() here because newer Windows SDK projections do not
        // always surface that extension on the projected interface type at compile time.
        var access = winRtSurface.NativeObject.AsInterface<IDirect3DDxgiInterfaceAccess>();
        var iid = Texture2DGuid;
        access.GetInterface(ref iid, out var texturePtr);
        return texturePtr;
    }

    private static IntPtr GetSharedHandle()
    {
        lock (_d3dLock)
        {
            if (_sharedHandle != IntPtr.Zero) return _sharedHandle;
            if (_sharedTexture is null) return IntPtr.Zero;
            using var resource = _sharedTexture.QueryInterface<DxgiResource>();
            _sharedHandle = resource.SharedHandle;
            return _sharedHandle;
        }
    }

    private static void EnsureSharedTextureSizeLocked(int width, int height, string reason)
    {
        width = Math.Clamp(width, 16, 8192);
        height = Math.Clamp(height, 16, 8192);
        if (_sharedTexture is not null && _sharedTextureWidth == width && _sharedTextureHeight == height) return;

        // Once Direct Stream has handed the shared texture handle to BridgeHost or to the plugin-side
        // receiver display, that handle must remain live and actively updated. Recreating the texture
        // during WGC/window-size churn leaves the other process opening a disposed/stale handle, which
        // shows up as E_INVALIDARG and a black receiver screen. Keep the existing shared texture stable
        // while Direct Stream is starting/active and let the CopySubresourceRegion safety path copy the
        // live frame into the existing stream texture.
        if (_sharedTexture is not null && DirectStreamBridge.IsPublisherUsingCurrentSharedTexture)
        {
            if (!_directStreamTextureSizeLockNoticeSent)
            {
                _directStreamTextureSizeLockNoticeSent = true;
                Send(new { op = "status", text = $"Direct Stream locked shared texture at {_sharedTextureWidth}x{_sharedTextureHeight}; capture frame is {width}x{height}." });
            }
            return;
        }

        _directStreamTextureSizeLockNoticeSent = false;
        RecreateSharedTextureLocked(width, height);
        PublishSharedTexture($"matched to {reason}");
    }

    private static void RecreateSharedTextureLocked(int width, int height)
    {
        if (_d3dDevice is null) throw new InvalidOperationException("D3D device was not initialised.");

        var old = _sharedTexture;
        _sharedHandle = IntPtr.Zero;
        _sharedTextureWidth = Math.Clamp(width, 16, 8192);
        _sharedTextureHeight = Math.Clamp(height, 16, 8192);
        _sharedTexture = new D3DTexture2D(_d3dDevice, new Texture2DDescription
        {
            Width = _sharedTextureWidth,
            Height = _sharedTextureHeight,
            MipLevels = 1,
            ArraySize = 1,
            Format = Format.B8G8R8A8_UNorm,
            SampleDescription = new SampleDescription(1, 0),
            Usage = ResourceUsage.Default,
            BindFlags = BindFlags.ShaderResource,
            CpuAccessFlags = CpuAccessFlags.None,
            OptionFlags = ResourceOptionFlags.Shared
        });

        try { old?.Dispose(); } catch { }
    }

    private static (int Width, int Height) GetSharedTextureSize()
    {
        lock (_d3dLock)
            return (_sharedTextureWidth, _sharedTextureHeight);
    }

    private static void PublishSharedTexture(string reason)
    {
        var (width, height) = GetSharedTextureSize();
        Send(new { op = "sharedTexture", source = "webview", handle = GetSharedHandle().ToInt64(), width, height });
        if (!string.IsNullOrWhiteSpace(reason))
            Send(new { op = "status", text = $"WebView2 shared texture ready: {reason} ({width}x{height})" });
    }

    private static bool TryPublishDirectStreamReceiverTexture(IntPtr receiverSharedHandle, int width, int height, out IntPtr publishedHandle, out int publishedWidth, out int publishedHeight, out string? error)
    {
        publishedHandle = IntPtr.Zero;
        publishedWidth = 0;
        publishedHeight = 0;
        error = null;

        if (receiverSharedHandle == IntPtr.Zero)
        {
            error = "Direct Stream receiver sent an empty texture handle.";
            return false;
        }

        width = Math.Clamp(width <= 0 ? _width : width, 16, 8192);
        height = Math.Clamp(height <= 0 ? _height : height, 16, 8192);
        if ((width & 1) == 1) width--;
        if ((height & 1) == 1) height--;

        lock (_d3dLock)
        {
            if (_d3dDevice is null || _d3dContext is null)
            {
                error = "Renderer D3D device is not ready yet.";
                return false;
            }

            try
            {
                var mayResizeForFirstReceiverFrame = DirectStreamBridge.IsReceiverWaitingForFirstSharedTexture;
                if (_sharedTexture is null || (_sharedTextureWidth != width || _sharedTextureHeight != height) && (!DirectStreamBridge.IsPublisherUsingCurrentSharedTexture || mayResizeForFirstReceiverFrame))
                    RecreateSharedTextureLocked(width, height);

                if (_sharedTexture is null)
                {
                    error = "Renderer shared texture is not ready yet.";
                    return false;
                }

                if (DirectStreamBridge.ReceiverSourceTextureHandle != receiverSharedHandle || DirectStreamBridge.ReceiverSourceTexture is null)
                {
                    DirectStreamBridge.ReleaseReceiverSourceTextureLocked();
                    DirectStreamBridge.ReceiverSourceTexture = _d3dDevice.OpenSharedResource<D3DTexture2D>(receiverSharedHandle);
                    DirectStreamBridge.ReceiverSourceTextureHandle = receiverSharedHandle;
                }

                var sourceTexture = DirectStreamBridge.ReceiverSourceTexture;
                if (sourceTexture is null)
                {
                    error = "Direct Stream receiver source texture is not ready yet.";
                    return false;
                }

                var sourceDesc = sourceTexture.Description;
                if (sourceDesc.Width == _sharedTextureWidth && sourceDesc.Height == _sharedTextureHeight)
                {
                    _d3dContext.CopyResource(sourceTexture, _sharedTexture);
                }
                else
                {
                    var copyWidth = Math.Min(sourceDesc.Width, _sharedTextureWidth);
                    var copyHeight = Math.Min(sourceDesc.Height, _sharedTextureHeight);
                    if (copyWidth <= 0 || copyHeight <= 0)
                    {
                        error = $"Direct Stream receiver texture had invalid dimensions {sourceDesc.Width}x{sourceDesc.Height}.";
                        return false;
                    }

                    var region = new ResourceRegion(0, 0, 0, copyWidth, copyHeight, 1);
                    _d3dContext.CopySubresourceRegion(sourceTexture, 0, region, _sharedTexture, 0, 0, 0, 0);
                }

                // Unlike the local WGC preview path, Direct Stream receiver frames arrive from a
                // separate BridgeHost process and are immediately handed onward to the plugin as a
                // shared texture. Flush this copy so the game/plugin side does not sample the old
                // blank/grey contents while the command queue catches up.
                _d3dContext.Flush();

                publishedHandle = GetSharedHandle();
                publishedWidth = _sharedTextureWidth;
                publishedHeight = _sharedTextureHeight;
                return publishedHandle != IntPtr.Zero;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                DirectStreamBridge.ReleaseReceiverSourceTextureLocked();
                return false;
            }
        }
    }

    private static void StartBrowserCapture()
    {
        if (_browserCaptureMethod == BrowserCaptureMethod.Wgc)
        {
            if (_form is null) throw new InvalidOperationException("RavaCast WebView2 host form was not initialised.");
            StartWindowCapture(_form.Handle);
            return;
        }

        if (_browserCaptureMethod == BrowserCaptureMethod.CapturePreview)
        {
            StartCapturePreviewFallback("selected by capture mode");
            return;
        }

        StartGdiWindowCapture();
    }

    private static void StartWindowCapture(IntPtr hwnd)
    {
        if (_winRtDevice is null) throw new InvalidOperationException("WinRT D3D device was not initialised.");
        if (!GraphicsCaptureSession.IsSupported()) throw new InvalidOperationException("Windows Graphics Capture is not supported on this system.");

        ShutdownCapture();
        _captureItem = CreateItemForWindow(hwnd) ?? throw new InvalidOperationException("Could not create WGC capture item for RavaCast WebView2 host window.");
        var size = _captureItem.Size;
        if (size.Width <= 0 || size.Height <= 0)
            size = new SizeInt32 { Width = _width, Height = _height };
        _captureWidth = Math.Max(1, size.Width);
        _captureHeight = Math.Max(1, size.Height);
        lock (_d3dLock)
            EnsureSharedTextureSizeLocked(_captureWidth, _captureHeight, "WGC capture item");
        PublishSharedTexture("WGC capture item");
        RefreshCursorSterileChildSubclasses();

        _framePool = Direct3D11CaptureFramePool.CreateFreeThreaded(_winRtDevice, DirectXPixelFormat.B8G8R8A8UIntNormalized, 3, size);
        _framePool.FrameArrived += OnFrameArrived;
        _captureSession = _framePool.CreateCaptureSession(_captureItem);
        TrySetOptionalCaptureProperty(_captureSession, "IsCursorCaptureEnabled", false);
        TrySetOptionalCaptureProperty(_captureSession, "IsBorderRequired", false);
        _captureSession.StartCapture();
        _captureReady = true;
        Send(new { op = "status", text = $"WebView2 WGC capture active ({_captureWidth}x{_captureHeight})" });
    }

    private static void StartGdiWindowCapture()
    {
        if (_form is null || _webView is null) throw new InvalidOperationException("RavaCast WebView2 host was not initialised.");

        ShutdownCapture();
        _gdiCaptureHwnd = ResolveGdiCaptureTarget();
        if (_gdiCaptureHwnd == IntPtr.Zero) _gdiCaptureHwnd = _form.Handle;
        RefreshCursorSterileChildSubclasses();

        var targetClass = GetWindowClassName(_gdiCaptureHwnd);
        var (width, height) = GetClientSizeForHwnd(_gdiCaptureHwnd);
        Send(new { op = "status", text = $"WebView2 GDI capture target: {targetClass} ({width}x{height})" });
        _captureWidth = width;
        _captureHeight = height;
        lock (_d3dLock)
            EnsureSharedTextureSizeLocked(_captureWidth, _captureHeight, "GDI window capture item");
        PublishSharedTexture("GDI window capture item");

        _sentFirstGdiFrame = false;
        _trustedNonBlankGdiFrames = 0;
        Interlocked.Exchange(ref _lastTrustedNonBlankGdiFrameTick, 0);
        StartGdiCaptureLoop();
        _captureReady = true;
        Send(new { op = "status", text = $"WebView2 OBS-style GDI window capture active ({_captureWidth}x{_captureHeight}); method={_gdiWindowCaptureMethod}" });
    }

    private static void StartCapturePreviewFallback(string reason)
    {
        if (_core is null) throw new InvalidOperationException("RavaCast WebView2 browser was not initialised.");

        ShutdownCapture();
        lock (_d3dLock)
            EnsureSharedTextureSizeLocked(_width, _height, "WebView2 preview fallback");
        PublishSharedTexture("WebView2 preview fallback");

        _usingCapturePreviewFallback = true;
        _capturePreviewInFlight = false;
        _capturePreviewTimer = new WinFormsTimer { Interval = 16 };
        _capturePreviewTimer.Tick += async (_, _) => await CaptureWebViewPreviewFrameAsync().ConfigureAwait(true);
        _capturePreviewTimer.Start();
        _captureReady = true;
        Send(new { op = "status", text = "WebView2 preview fallback active", detail = reason });
    }

    private static async Task CaptureWebViewPreviewFrameAsync()
    {
        if (_shutdownRequested || _core is null || _d3dContext is null || _capturePreviewInFlight) return;
        if (DirectStreamBridge.IsReceiverUsingHostVideoTexture) return;

        _capturePreviewInFlight = true;
        try
        {
            using var stream = new MemoryStream();
            await _core.CapturePreviewAsync(CoreWebView2CapturePreviewImageFormat.Png, stream).ConfigureAwait(true);
            if (stream.Length <= 0) return;

            stream.Position = 0;
            using var image = Image.FromStream(stream);
            if (image.Width <= 0 || image.Height <= 0) return;

            using var bitmap = new Bitmap(image.Width, image.Height, PixelFormat.Format32bppArgb);
            using (var graphics = Graphics.FromImage(bitmap))
                graphics.DrawImage(image, 0, 0, image.Width, image.Height);

            if (IsBitmapVisiblyBlank(bitmap))
            {
                var nowWarn = Environment.TickCount64;
                if (nowWarn - Interlocked.Read(ref _lastGdiBlankWarningTick) > 3000)
                {
                    Interlocked.Exchange(ref _lastGdiBlankWarningTick, nowWarn);
                    Send(new { op = "status", text = "WebView2 preview fallback is still receiving blank browser frames" });
                }
                return;
            }

            lock (_d3dLock)
            {
                EnsureSharedTextureSizeLocked(bitmap.Width, bitmap.Height, "WebView2 preview fallback frame");
                if (_sharedTexture is null) return;
            }

            UploadBitmapToSharedTexture(bitmap);

            var frameIndex = Interlocked.Increment(ref _frameIndex);
            if (!_sentFirstFrame)
            {
                _sentFirstFrame = true;
                Send(new { op = "status", text = "WebView2 first preview-fallback frame received" });
            }

            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastFrameMessageTick) >= 16)
            {
                Interlocked.Exchange(ref _lastFrameMessageTick, now);
                Send(new { op = "frame", source = "webview", frame = frameIndex, width = _sharedTextureWidth, height = _sharedTextureHeight });
            }
        }
        catch (Exception ex)
        {
            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastCapturePreviewErrorTick) > 3000)
            {
                Interlocked.Exchange(ref _lastCapturePreviewErrorTick, now);
                Send(new { op = "error", message = "WebView2 preview fallback capture failed: " + ex.Message });
            }
        }
        finally
        {
            _capturePreviewInFlight = false;
        }
    }

    private static IntPtr ResolveGdiCaptureTarget()
    {
        try
        {
            // OBS-style Window Capture captures the top-level window, not the innermost Chromium
            // child HWND.  Capturing Chrome_RenderWidgetHostHWND with GDI can report success while
            // returning an all-black client DC, especially with WebView2/Chromium composition.
            // Default to the real RavaCast browser window and keep child targets as diagnostic
            // escape hatches only.
            var targetMode = Environment.GetEnvironmentVariable("RAVACAST_GDI_CAPTURE_TARGET")?.Trim().ToLowerInvariant();
            if (targetMode is "chromium" or "render" or "child")
            {
                var webViewHwnd = _webView?.Handle ?? IntPtr.Zero;
                if (webViewHwnd != IntPtr.Zero)
                {
                    var chromium = TryFindChromiumInputChildHwnd(webViewHwnd);
                    if (chromium != IntPtr.Zero) return chromium;
                    return webViewHwnd;
                }
            }

            if (targetMode is "webview" or "control")
            {
                var webViewHwnd = _webView?.Handle ?? IntPtr.Zero;
                if (webViewHwnd != IntPtr.Zero) return webViewHwnd;
            }

            if (_form is not null && _form.Handle != IntPtr.Zero)
                return _form.Handle;
        }
        catch { }

        return _form?.Handle ?? IntPtr.Zero;
    }

    private static void StartGdiCaptureLoop()
    {
        try { _gdiCaptureLoopCts?.Cancel(); } catch { }
        try { _gdiCaptureLoopCts?.Dispose(); } catch { }

        var cts = new CancellationTokenSource();
        _gdiCaptureLoopCts = cts;
        _gdiCaptureLoopTask = Task.Run(() => GdiCaptureLoopAsync(cts.Token), CancellationToken.None);
    }

    private static async Task GdiCaptureLoopAsync(CancellationToken token)
    {
        var targetFps = 60;
        var frameDelayMs = 1000.0 / targetFps;
        var clock = Stopwatch.StartNew();
        var nextFrameMs = 0.0;

        while (!token.IsCancellationRequested && !_shutdownRequested)
        {
            try
            {
                var nowMs = clock.Elapsed.TotalMilliseconds;
                if (nowMs < nextFrameMs)
                {
                    var delay = TimeSpan.FromMilliseconds(Math.Max(1, nextFrameMs - nowMs));
                    await Task.Delay(delay, token).ConfigureAwait(false);
                    nowMs = clock.Elapsed.TotalMilliseconds;
                }

                if (nextFrameMs < nowMs - frameDelayMs)
                    nextFrameMs = nowMs;
                nextFrameMs += frameDelayMs;

                CaptureGdiWindowFrame();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                var nowWarn = Environment.TickCount64;
                if (nowWarn - Interlocked.Read(ref _lastGdiCaptureLoopWarningTick) > 3000)
                {
                    Interlocked.Exchange(ref _lastGdiCaptureLoopWarningTick, nowWarn);
                    Send(new { op = "error", message = "WebView2 GDI capture loop failed: " + ex.Message });
                }

                try { await Task.Delay(50, token).ConfigureAwait(false); } catch { break; }
            }
        }
    }

    private static void CaptureGdiWindowFrame()
    {
        if (_shutdownRequested || _d3dContext is null) return;
        if (DirectStreamBridge.IsReceiverUsingHostVideoTexture) return;

        lock (_gdiCaptureSync)
        {
            try
            {
                RefreshGdiCaptureTargetIfNeeded();
                var hwnd = _gdiCaptureHwnd;
                if (hwnd == IntPtr.Zero || !IsWindow(hwnd)) return;

                var (width, height) = GetClientSizeForHwnd(hwnd);
                if (width <= 0 || height <= 0) return;

                lock (_d3dLock)
                {
                    EnsureSharedTextureSizeLocked(width, height, "GDI window frame");
                    if (_sharedTexture is null) return;
                }

                EnsureGdiCaptureBitmap(width, height);
                if (_gdiCaptureBitmap is null) return;

                var captured = CaptureHwndIntoBitmap(hwnd, _gdiCaptureBitmap);
                var blank = captured && IsBitmapVisiblyBlank(_gdiCaptureBitmap);
                if (!captured || blank && !CanAcceptTemporaryBlankGdiFrame())
                {
                    var blankFrames = Interlocked.Increment(ref _consecutiveBlankGdiFrames);
                    var nowWarn = Environment.TickCount64;
                    if (nowWarn - Interlocked.Read(ref _lastGdiWarningTick) > 3000)
                    {
                        Interlocked.Exchange(ref _lastGdiWarningTick, nowWarn);
                        Send(new
                        {
                            op = "status",
                            text = blank
                                ? "WebView2 GDI capture is ignoring black browser frames until real content has settled"
                                : "WebView2 GDI window capture is waiting for a drawable browser surface",
                            detail = $"blankOrFailedFrames={blankFrames}; trustedNonBlank={Volatile.Read(ref _trustedNonBlankGdiFrames)}"
                        });
                    }
                    return;
                }

                if (!blank)
                    MarkTrustedNonBlankGdiFrame();

                Interlocked.Exchange(ref _consecutiveBlankGdiFrames, 0);
                UploadBitmapToSharedTexture(_gdiCaptureBitmap);

                var frameIndex = Interlocked.Increment(ref _frameIndex);
                if (!_sentFirstGdiFrame)
                {
                    _sentFirstGdiFrame = true;
                    _sentFirstFrame = true;
                    Send(new { op = "status", text = "WebView2 first GDI window-capture frame received" });
                }

                var now = Environment.TickCount64;
                if (now - Interlocked.Read(ref _lastFrameMessageTick) >= 16)
                {
                    Interlocked.Exchange(ref _lastFrameMessageTick, now);
                    Send(new { op = "frame", source = "webview", frame = frameIndex, width = _sharedTextureWidth, height = _sharedTextureHeight });
                }
            }
            catch (Exception ex)
            {
                Send(new { op = "error", message = "WebView2 GDI window capture failed: " + ex.Message });
            }
        }
    }

    private static void RefreshGdiCaptureTargetIfNeeded()
    {
        var now = Environment.TickCount64;
        if (now - Interlocked.Read(ref _lastGdiTargetProbeTick) < 1500) return;
        Interlocked.Exchange(ref _lastGdiTargetProbeTick, now);

        var form = _form;
        if (form is null || form.IsDisposed) return;

        try
        {
            if (form.InvokeRequired)
            {
                form.BeginInvoke(new Action(RefreshGdiCaptureTargetOnUiThread));
                return;
            }
        }
        catch
        {
            return;
        }

        RefreshGdiCaptureTargetOnUiThread();
    }

    private static void RefreshGdiCaptureTargetOnUiThread()
    {
        var target = ResolveGdiCaptureTarget();
        if (target == IntPtr.Zero || target == _gdiCaptureHwnd) return;

        _gdiCaptureHwnd = target;
        RefreshCursorSterileChildSubclasses();
        var className = GetWindowClassName(target);
        var (width, height) = GetClientSizeForHwnd(target);
        Send(new { op = "status", text = $"WebView2 GDI capture target: {className} ({width}x{height})" });
    }

    private static void EnsureGdiCaptureBitmap(int width, int height)
    {
        width = Math.Clamp(width, 16, 8192);
        height = Math.Clamp(height, 16, 8192);
        if (_gdiCaptureBitmap is not null && _gdiCaptureBitmap.Width == width && _gdiCaptureBitmap.Height == height) return;

        try { _gdiCaptureBitmap?.Dispose(); } catch { }
        _gdiCaptureBitmap = new Bitmap(width, height, PixelFormat.Format32bppArgb);
    }

    private static bool CaptureHwndIntoBitmap(IntPtr hwnd, Bitmap bitmap)
    {
        try
        {
            return _gdiWindowCaptureMethod switch
            {
                GdiWindowCaptureMethod.BitBlt => CaptureHwndIntoBitmapWithMethod(hwnd, bitmap, usePrintWindow: false),
                GdiWindowCaptureMethod.PrintWindow => CaptureHwndIntoBitmapWithMethod(hwnd, bitmap, usePrintWindow: true),
                _ => CaptureHwndIntoBitmapAuto(hwnd, bitmap),
            };
        }
        catch
        {
            return false;
        }
    }

    private static bool CaptureHwndIntoBitmapAuto(IntPtr hwnd, Bitmap bitmap)
    {
        // BitBlt can report success for WebView2/Chromium while returning an empty black surface.
        // Never let a black BitBlt frame win before PrintWindow/top-level PrintWindow have had a chance
        // to find real browser pixels. Only after this capture path has proved itself with real frames do
        // we allow short, temporary black runs for genuine fade-to-black video content.
        var sawBlank = false;

        if (CaptureHwndIntoBitmapWithMethod(hwnd, bitmap, usePrintWindow: false))
        {
            if (!IsBitmapVisiblyBlank(bitmap)) return true;
            sawBlank = true;
        }

        var nowFallback = Environment.TickCount64;
        if (nowFallback - Interlocked.Read(ref _lastGdiAutoFallbackTick) > 3000)
        {
            Interlocked.Exchange(ref _lastGdiAutoFallbackTick, nowFallback);
            Send(new { op = "status", text = "WebView2 GDI BitBlt frame was blank; trying PrintWindow fallback" });
        }

        if (CaptureHwndIntoBitmapWithMethod(hwnd, bitmap, usePrintWindow: true))
        {
            if (!IsBitmapVisiblyBlank(bitmap)) return true;
            sawBlank = true;
        }

        var topLevel = _form?.Handle ?? IntPtr.Zero;
        if (topLevel != IntPtr.Zero && topLevel != hwnd && IsWindow(topLevel))
        {
            if (CaptureHwndIntoBitmapWithMethod(topLevel, bitmap, usePrintWindow: true))
            {
                if (!IsBitmapVisiblyBlank(bitmap))
                {
                    _gdiCaptureHwnd = topLevel;
                    Send(new { op = "status", text = "WebView2 GDI capture switched to the top-level browser window after blank child frames" });
                    return true;
                }

                sawBlank = true;
            }
        }

        if (sawBlank && CanAcceptTemporaryBlankGdiFrame())
            return true;

        var nowWarn = Environment.TickCount64;
        if (nowWarn - Interlocked.Read(ref _lastGdiBlankWarningTick) > 3000)
        {
            Interlocked.Exchange(ref _lastGdiBlankWarningTick, nowWarn);
            Send(new { op = "status", text = "WebView2 GDI capture is receiving blank frames from the browser window" });
        }

        return false;
    }

    private static void MarkTrustedNonBlankGdiFrame()
    {
        if (Volatile.Read(ref _trustedNonBlankGdiFrames) < 3)
            Interlocked.Increment(ref _trustedNonBlankGdiFrames);
        Interlocked.Exchange(ref _lastTrustedNonBlankGdiFrameTick, Environment.TickCount64);
    }

    private static bool CanAcceptTemporaryBlankGdiFrame()
    {
        if (Volatile.Read(ref _trustedNonBlankGdiFrames) < 3) return false;
        var lastRealFrame = Interlocked.Read(ref _lastTrustedNonBlankGdiFrameTick);
        if (lastRealFrame <= 0) return false;
        return Environment.TickCount64 - lastRealFrame <= 5000;
    }

    private static bool CaptureHwndIntoBitmapWithMethod(IntPtr hwnd, Bitmap bitmap, bool usePrintWindow)
    {
        using var graphics = Graphics.FromImage(bitmap);
        graphics.Clear(Color.Black);
        var hdcDest = graphics.GetHdc();
        try
        {
            return usePrintWindow
                ? PrintWindow(hwnd, hdcDest, PW_RENDERFULLCONTENT)
                : CaptureHwndByBitBlt(hwnd, hdcDest, bitmap.Width, bitmap.Height);
        }
        finally
        {
            graphics.ReleaseHdc(hdcDest);
        }
    }

    private static unsafe bool IsBitmapVisiblyBlank(Bitmap bitmap)
    {
        BitmapData? data = null;
        try
        {
            data = bitmap.LockBits(new Rectangle(0, 0, bitmap.Width, bitmap.Height), ImageLockMode.ReadOnly, PixelFormat.Format32bppArgb);
            if (data.Scan0 == IntPtr.Zero || bitmap.Width <= 0 || bitmap.Height <= 0) return true;

            var width = bitmap.Width;
            var height = bitmap.Height;
            var stride = data.Stride;
            var basePtr = (byte*)data.Scan0;
            var xStep = Math.Max(1, width / 32);
            var yStep = Math.Max(1, height / 18);
            var samples = 0;
            var visible = 0;

            for (var y = 0; y < height; y += yStep)
            {
                var row = basePtr + (y * stride);
                for (var x = 0; x < width; x += xStep)
                {
                    var p = row + (x * BytesPerPixel);
                    var b = p[0];
                    var g = p[1];
                    var r = p[2];
                    samples++;

                    // Allow very dark content, but reject the common all-black/all-zero capture failure.
                    if (r > 8 || g > 8 || b > 8)
                        visible++;
                }
            }

            return samples == 0 || visible < Math.Max(2, samples / 128);
        }
        catch
        {
            return true;
        }
        finally
        {
            if (data is not null)
                bitmap.UnlockBits(data);
        }
    }

    private static bool CaptureHwndByBitBlt(IntPtr hwnd, IntPtr hdcDest, int width, int height)
    {
        var hdcSource = GetDC(hwnd);
        if (hdcSource == IntPtr.Zero) hdcSource = GetWindowDC(hwnd);
        if (hdcSource == IntPtr.Zero) return false;

        try
        {
            // Keep the default capture path passive, like OBS Window Capture with cursor capture off.
            // CAPTUREBLT can involve layered/overlaid window composition and has been observed to make
            // the real Windows pointer blink system-wide while RavaCast is running.  Leave it as an
            // explicit diagnostic fallback only.
            var rop = SRCCOPY;
            if (IsEnvEnabled("RAVACAST_GDI_USE_CAPTUREBLT") || IsEnvEnabled("RAVACAST_GDI_CAPTURE_LAYERED_WINDOWS"))
                rop |= CAPTUREBLT;

            return BitBlt(hdcDest, 0, 0, width, height, hdcSource, 0, 0, rop);
        }
        finally
        {
            ReleaseDC(hwnd, hdcSource);
        }
    }

    private static void UploadBitmapToSharedTexture(Bitmap bitmap)
    {
        if (_sharedTexture is null || _d3dContext is null) return;
        if (DirectStreamBridge.IsReceiverUsingHostVideoTexture) return;

        BitmapData? data = null;
        try
        {
            data = bitmap.LockBits(new Rectangle(0, 0, bitmap.Width, bitmap.Height), ImageLockMode.ReadWrite, PixelFormat.Format32bppArgb);
            ForceBitmapAlphaOpaque(data, bitmap.Width, bitmap.Height);
            lock (_d3dLock)
            {
                if (_sharedTexture is null || _d3dContext is null) return;
                _d3dContext.UpdateSubresource(new DataBox(data.Scan0, data.Stride, 0), _sharedTexture, 0);
                _d3dContext.Flush();
            }
        }
        finally
        {
            if (data is not null)
                bitmap.UnlockBits(data);
        }
    }

    private static unsafe void ForceBitmapAlphaOpaque(BitmapData data, int width, int height)
    {
        if (data.Scan0 == IntPtr.Zero || width <= 0 || height <= 0) return;

        var stride = data.Stride;
        var row = (byte*)data.Scan0;
        for (var y = 0; y < height; y++)
        {
            var pixel = row + (y * stride);
            for (var x = 0; x < width; x++)
                pixel[(x * BytesPerPixel) + 3] = 255;
        }
    }

    private static (int Width, int Height) GetClientSizeForHwnd(IntPtr hwnd)
    {
        if (hwnd != IntPtr.Zero && GetClientRect(hwnd, out var clientRect))
        {
            var width = Math.Max(1, clientRect.Right - clientRect.Left);
            var height = Math.Max(1, clientRect.Bottom - clientRect.Top);
            if (width > 1 && height > 1) return (width, height);
        }

        if (hwnd != IntPtr.Zero && GetWindowRect(hwnd, out var windowRect))
        {
            var width = Math.Max(1, windowRect.Right - windowRect.Left);
            var height = Math.Max(1, windowRect.Bottom - windowRect.Top);
            if (width > 1 && height > 1) return (width, height);
        }

        return (Math.Max(1, _width), Math.Max(1, _height));
    }

    private static void OnFrameArrived(Direct3D11CaptureFramePool sender, object args)
    {
        if (_shutdownRequested || _sharedTexture is null || _d3dContext is null) return;
        if (DirectStreamBridge.IsReceiverUsingHostVideoTexture) return;

        try
        {
            using var frame = sender.TryGetNextFrame();
            if (frame is null) return;

            var surface = frame.Surface;
            var texturePtr = GetTexturePointerFromDirect3DSurface(surface);
            if (texturePtr == IntPtr.Zero) return;

            using var sourceTexture = new D3DTexture2D(texturePtr);
            lock (_d3dLock)
            {
                var desc = sourceTexture.Description;
                _captureWidth = Math.Max(1, desc.Width);
                _captureHeight = Math.Max(1, desc.Height);
                EnsureSharedTextureSizeLocked(_captureWidth, _captureHeight, "WGC frame");

                if (_sharedTexture is null) return;
                if (desc.Width == _sharedTextureWidth && desc.Height == _sharedTextureHeight)
                {
                    _d3dContext.CopyResource(sourceTexture, _sharedTexture);
                }
                else
                {
                    // Last-resort safety path only. The normal path above recreates the shared
                    // texture to the WGC frame size so the preview is never cropped, stretched,
                    // or offset versus the browser input surface.
                    var copyWidth = Math.Min(desc.Width, _sharedTextureWidth);
                    var copyHeight = Math.Min(desc.Height, _sharedTextureHeight);
                    if (copyWidth > 0 && copyHeight > 0)
                    {
                        var region = new ResourceRegion(0, 0, 0, copyWidth, copyHeight, 1);
                        _d3dContext.CopySubresourceRegion(sourceTexture, 0, region, _sharedTexture, 0, 0, 0, 0);
                    }
                }
                // Avoid forcing a GPU flush every WGC frame. Consumers can naturally see the next frame;
                // forcing the command stream here increases cross-process/shared-texture GPU pressure and
                // showed up as host-side Direct Stream stutter on some machines.
            }

            var frameIndex = Interlocked.Increment(ref _frameIndex);
            if (!_sentFirstFrame)
            {
                _sentFirstFrame = true;
                Send(new { op = "status", text = "WebView2 first WGC frame received" });
            }

            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastFrameMessageTick) >= 16)
            {
                Interlocked.Exchange(ref _lastFrameMessageTick, now);
                Send(new { op = "frame", source = "webview", frame = frameIndex, width = _sharedTextureWidth, height = _sharedTextureHeight });
            }
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 WGC frame copy failed: " + ex.Message });
        }
    }

    private static void CommandLoop()
    {
        try
        {
            while (_reader is not null && !_shutdownRequested)
            {
                var line = _reader.ReadLine();
                if (line is null) break;
                LogRenderer("IN " + line);
                _pendingCommands.Enqueue(line);
            }
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 IPC read failed: " + ex.Message });
        }
        finally
        {
            _shutdownRequested = true;
            try { _form?.BeginInvoke(new Action(() => _form.Close())); } catch { }
        }
    }

    private static void SendLoop()
    {
        while (!_shutdownRequested || !_outgoingMessages.IsEmpty)
        {
            if (_outgoingMessages.TryDequeue(out var json))
            {
                try
                {
                    lock (_sendLock)
                    {
                        _writer?.WriteLine(json);
                    }
                }
                catch
                {
                    if (_shutdownRequested) return;
                }
                continue;
            }
            try { _sendSignal.WaitOne(50); } catch { return; }
        }
    }

    private static void PumpPendingCommands()
    {
        while (_pendingCommands.TryDequeue(out var line))
        {
            try { HandleCommand(line); }
            catch (Exception ex) { Send(new { op = "error", message = "WebView2 command failed: " + ex.Message }); }
        }

        if (_webViewReady && _captureReady && !_sentFirstFrame && _frameIndex == 0)
        {
            // Keep the browser compositor awake while pages are loading or static.
            try { _webView?.Invalidate(); } catch { }
        }
    }

    private static void HandleCommand(string line)
    {
        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;
        var op = root.TryGetProperty("op", out var opProp) ? opProp.GetString() : string.Empty;
        switch (op)
        {
            case "navigate":
                if (root.TryGetProperty("url", out var url) && !string.IsNullOrWhiteSpace(url.GetString()))
                    Navigate(url.GetString()!);
                break;
            case "play":
            case "pause":
            case "seek":
                if (root.TryGetProperty("positionSeconds", out var legacyPos))
                    QueueMediaSync(legacyPos.GetDouble(), isPlaying: true, force: true);
                break;
            case "syncMedia":
                var syncPosition = root.TryGetProperty("positionSeconds", out var posProp) ? posProp.GetDouble() : 0.0;
                var syncPlaying = !root.TryGetProperty("isPlaying", out var playingProp) || playingProp.GetBoolean();
                var syncForce = root.TryGetProperty("force", out var forceProp) && forceProp.GetBoolean();
                QueueMediaSync(syncPosition, syncPlaying, syncForce);
                break;
            case "audio":
                if (root.TryGetProperty("muted", out var muted)) _muted = muted.GetBoolean();
                if (root.TryGetProperty("volume", out var volume)) _volume = Math.Clamp((float)volume.GetDouble(), 0f, 1f);
                ApplyAudioState();
                DirectStreamBridge.SetAudio(_muted, _volume);
                break;
            case "importConsentCookies":
                _ = ImportConsentCookiesAsync(root);
                break;
            case "mouse":
                ApplyMousePacket(root);
                break;
            case "focus":
                if (!root.TryGetProperty("focused", out var focused) || focused.GetBoolean()) FocusBrowser();
                break;
            case "mouseMove":
                ApplyLegacyMouseMoveCommand(root);
                break;
            case "mouseClick":
                ApplyLegacyMouseClickCommand(root);
                break;
            case "mouseWheel":
                ApplyLegacyMouseWheelCommand(root);
                break;
            case "textInput":
                if (root.TryGetProperty("text", out var textProp)) SendText(textProp.GetString() ?? string.Empty);
                break;
            case "key":
                ApplyKeyPacket(root);
                break;
            case "specialKey":
                if (root.TryGetProperty("key", out var keyProp)) SendSpecialKey(keyProp.GetString() ?? string.Empty);
                break;
            case "reload":
                _core?.Reload();
                Send(new { op = "status", text = "WebView2 reload requested" });
                break;
            case "back":
                if (_core?.CanGoBack == true) _core.GoBack();
                Send(new { op = "status", text = "WebView2 back requested" });
                break;
            case "directStreamStartPublisher":
                var (sourceWidth, sourceHeight) = GetSharedTextureSize();
                DirectStreamBridge.StartPublisher(root, GetSharedHandle(), sourceWidth, sourceHeight);
                break;
            case "directStreamStopPublisher":
                DirectStreamBridge.StopPublisher("Publisher stopped by plugin");
                break;
            case "directStreamStartReceiver":
                DirectStreamBridge.StartReceiver(root);
                break;
            case "directStreamStopReceiver":
                DirectStreamBridge.StopReceiver("Receiver stopped by plugin");
                break;
            case "directStreamAddPeer":
                DirectStreamBridge.AddPeer(root);
                break;
            case "directStreamRemovePeer":
                DirectStreamBridge.RemovePeer(root);
                break;
            case "directStreamSignal":
                DirectStreamBridge.HandleSignal(root);
                break;
            case "showWindow":
                ShowInteractiveBrowserWindow();
                break;
            case "hideWindow":
                HideInteractiveBrowserWindow();
                break;
            case "stop":
                _currentUrl = null;
                NavigateToBlank();
                Send(new { op = "status", text = "WebView2 stopped" });
                break;
            case "quit":
                _shutdownRequested = true;
                Send(new { op = "status", text = "WebView2 renderer shutting down" });
                try { _form?.Close(); } catch { }
                break;
        }
    }


    private static async Task InstallRavaCastBrowserExtensionsAsync()
    {
        if (_core is null) return;

        try
        {
            var extensionFolders = FindRavaCastBrowserExtensionFolders();
            if (extensionFolders.Count == 0)
            {
                _ravaCastBrowserExtensionsInstalled = true;
                Send(new { op = "status", text = "WebView2 extensions enabled; no unpacked uBlock Origin/Ghostery folders found in RavaCast.Extensions" });
                return;
            }

            var loaded = 0;
            foreach (var extensionFolder in extensionFolders)
            {
                try
                {
                    var extension = await _core.Profile.AddBrowserExtensionAsync(extensionFolder).ConfigureAwait(true);
                    lock (_cleanBrowsingExtensionGate)
                    {
                        _cleanBrowsingExtensions.Add(extension);
                    }
                    try { await extension.EnableAsync(!_protectedMediaModeActive).ConfigureAwait(true); } catch { }
                    loaded++;
                    Send(new { op = "status", text = $"WebView2 extension loaded: {extension.Name}", detail = extensionFolder });
                }
                catch (Exception ex)
                {
                    Send(new { op = "error", message = $"WebView2 extension failed to load from '{extensionFolder}': {ex.Message}" });
                }
            }

            _ravaCastBrowserExtensionsInstalled = true;
            Send(new { op = "status", text = $"WebView2 extension loader finished ({loaded}/{extensionFolders.Count} loaded)" });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 extension loader failed: " + ex.Message });
        }
    }

    private static List<string> FindRavaCastBrowserExtensionFolders()
    {
        var results = new List<string>(2);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        static void AddIfValid(string? path, List<string> results, HashSet<string> seen)
        {
            if (string.IsNullOrWhiteSpace(path)) return;
            try
            {
                var fullPath = Path.GetFullPath(path);
                if (!Directory.Exists(fullPath)) return;
                if (!File.Exists(Path.Combine(fullPath, "manifest.json"))) return;
                if (!IsSupportedRavaCastExtension(fullPath)) return;
                if (seen.Add(fullPath)) results.Add(fullPath);
            }
            catch { }
        }

        static bool LooksLikeUBlockFolder(string path)
        {
            var name = Path.GetFileName(path) ?? string.Empty;
            var normalised = name.Replace(" ", "", StringComparison.OrdinalIgnoreCase)
                                 .Replace("-", "", StringComparison.OrdinalIgnoreCase)
                                 .Replace("_", "", StringComparison.OrdinalIgnoreCase)
                                 .Replace(".", "", StringComparison.OrdinalIgnoreCase)
                                 .ToLowerInvariant();
            return normalised.Contains("ublock", StringComparison.OrdinalIgnoreCase);
        }

        static void ScanRoot(string? root, List<string> results, HashSet<string> seen)
        {
            if (string.IsNullOrWhiteSpace(root)) return;
            try
            {
                var fullRoot = Path.GetFullPath(root);
                if (!Directory.Exists(fullRoot)) return;

                AddIfValid(Path.Combine(fullRoot, "Ghostery"), results, seen);
                AddIfValid(Path.Combine(fullRoot, "ghostery"), results, seen);

                // YouTube is miserable without uBlock Origin, so ship and load it by default.
                // Use RAVACAST_LOAD_UBLOCK=0/false/no/off only for diagnostics. Protected-media
                // navigations still pause all bundled blockers before the site loads.
                var uBlockSetting = Environment.GetEnvironmentVariable("RAVACAST_LOAD_UBLOCK");
                var loadUBlock = !string.Equals(uBlockSetting, "0", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(uBlockSetting, "false", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(uBlockSetting, "no", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(uBlockSetting, "off", StringComparison.OrdinalIgnoreCase);

                if (loadUBlock)
                {
                    AddIfValid(Path.Combine(fullRoot, "uBlockOrigin"), results, seen);
                    AddIfValid(Path.Combine(fullRoot, "uBlock Origin"), results, seen);
                    AddIfValid(Path.Combine(fullRoot, "ublock-origin"), results, seen);
                    AddIfValid(Path.Combine(fullRoot, "uBlock0"), results, seen);
                    AddIfValid(Path.Combine(fullRoot, "uBlock"), results, seen);
                }

                foreach (var child in Directory.EnumerateDirectories(fullRoot))
                {
                    if (!loadUBlock && LooksLikeUBlockFolder(child)) continue;
                    AddIfValid(child, results, seen);
                }
            }
            catch { }
        }

        var baseDir = AppContext.BaseDirectory;
        ScanRoot(Path.Combine(baseDir, "RavaCast.Extensions"), results, seen);
        ScanRoot(Path.Combine(baseDir, "Extensions"), results, seen);

        return results;
    }

    private static bool IsSupportedRavaCastExtension(string extensionFolder)
    {
        static bool LooksLikeSupportedName(string? value)
        {
            if (string.IsNullOrWhiteSpace(value)) return false;
            var normalised = value.Replace(" ", "", StringComparison.OrdinalIgnoreCase)
                                  .Replace("-", "", StringComparison.OrdinalIgnoreCase)
                                  .Replace("_", "", StringComparison.OrdinalIgnoreCase)
                                  .Replace(".", "", StringComparison.OrdinalIgnoreCase)
                                  .ToLowerInvariant();

            return normalised.Contains("ublock", StringComparison.OrdinalIgnoreCase)
                || normalised.Contains("ghostery", StringComparison.OrdinalIgnoreCase);
        }

        if (LooksLikeSupportedName(Path.GetFileName(extensionFolder))) return true;

        try
        {
            var manifestPath = Path.Combine(extensionFolder, "manifest.json");
            if (!File.Exists(manifestPath)) return false;
            var manifest = File.ReadAllText(manifestPath);
            return LooksLikeSupportedName(manifest);
        }
        catch
        {
            return false;
        }
    }

    private static async Task EnsureNormalBrowsingToolsInstalledAsync()
    {
        if (_core is null) return;
        if (!_ravaCastBrowserExtensionsInstalled)
            await InstallRavaCastBrowserExtensionsAsync().ConfigureAwait(true);
        if (!_cleanBrowsingModeInstalled)
            await InstallCleanBrowsingModeAsync().ConfigureAwait(true);
    }

    private static async Task InstallCleanBrowsingModeAsync()
    {
        if (_core is null || _cleanBrowsingModeInstalled) return;
        try
        {
            _core.AddWebResourceRequestedFilter("*", CoreWebView2WebResourceContext.All);
            _core.WebResourceRequested += (_, e) =>
            {
                try
                {
                    if (!ShouldBlockWebResource(e.Request.Uri, e.ResourceContext)) return;
                    var body = new MemoryStream(Array.Empty<byte>());
                    e.Response = _core.Environment.CreateWebResourceResponse(body, 204, "Blocked by RavaCast", "Content-Type: text/plain\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-store");
                }
                catch (Exception ex)
                {
                    Send(new { op = "error", message = "RavaCast clean browsing request filter failed: " + ex.Message });
                }
            };

            await _core.AddScriptToExecuteOnDocumentCreatedAsync(CleanBrowsingScript).ConfigureAwait(true);
            _cleanBrowsingModeInstalled = true;
            Send(new { op = "status", text = "WebView2 clean browsing mode active (YouTube clean-player layer enabled)" });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 clean browsing mode could not be enabled: " + ex.Message });
        }
    }

    private static void OnNewWindowRequested(object? sender, CoreWebView2NewWindowRequestedEventArgs e)
    {
        try
        {
            var uri = e.Uri;
            var protectedMediaPopup = _protectedMediaModeActive || IsProtectedMediaUrl(uri);
            e.Handled = true;

            if (string.IsNullOrWhiteSpace(uri))
            {
                Send(new { op = "status", text = protectedMediaPopup
                    ? "Protected media popup requested without a target URL; keeping it inside RavaCast"
                    : "RavaCast blocked popup without a target URL" });
                return;
            }

            if (!protectedMediaPopup && ShouldBlockWebResource(uri, CoreWebView2WebResourceContext.Other))
            {
                Send(new { op = "status", text = "RavaCast clean browsing blocked popup: " + uri });
                return;
            }

            if (Uri.TryCreate(uri, UriKind.Absolute, out var parsed)
                && (parsed.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) || parsed.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
            {
                _ = NavigateCoreAsync(uri);
                Send(new { op = "status", text = protectedMediaPopup
                    ? "Protected media popup/redirect opened in the current RavaCast tab: " + uri
                    : "RavaCast opened popup target in current tab: " + uri });
            }
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "RavaCast popup/new-window handler failed: " + ex.Message });
        }
    }

    private static bool ShouldBlockWebResource(string? rawUri, CoreWebView2WebResourceContext context)
    {
        // Never block the top-level document. Clean browsing is only for noisy subresources,
        // popup targets and tracker/ad payloads, so entering a URL should not randomly fail.
        if (context == CoreWebView2WebResourceContext.Document) return false;
        if (string.IsNullOrWhiteSpace(rawUri)) return false;
        if (!Uri.TryCreate(rawUri, UriKind.Absolute, out var uri)) return false;
        if (!string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)) return false;

        // Protected streaming services are allowed to load their full player/auth/DRM pipeline.
        // Blocking ad/tracker-looking subresources here can leave Netflix/Prime/etc stuck at
        // title selection or an inert play button. DRM itself is still enforced by Edge/WebView2.
        if (IsProtectedMediaUrl(rawUri) || (_protectedMediaModeActive && context is not CoreWebView2WebResourceContext.Document))
            return false;

        var host = uri.Host.ToLowerInvariant();
        foreach (var fragment in BlockedHostFragments)
        {
            if (host.Contains(fragment, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        var url = rawUri.ToLowerInvariant();
        if ((host.Equals("youtube.com", StringComparison.OrdinalIgnoreCase) || host.EndsWith(".youtube.com", StringComparison.OrdinalIgnoreCase) || host.EndsWith(".youtube-nocookie.com", StringComparison.OrdinalIgnoreCase) || host.EndsWith(".ytimg.com", StringComparison.OrdinalIgnoreCase))
            && BlockedYouTubeUrlFragments.Any(fragment => url.Contains(fragment, StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        foreach (var fragment in BlockedUrlFragments)
        {
            if (url.Contains(fragment, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }


    private static async Task InstallProtectedMediaDiagnosticsAsync()
    {
        if (_core is null || _protectedMediaDiagnosticsInstalled) return;

        static JsonDocument ParseDevToolsJson(string json)
        {
            return JsonDocument.Parse(System.Text.Encoding.UTF8.GetBytes(json ?? "{}"));
        }

        static string ReadString(JsonElement element, string name)
            => element.ValueKind != JsonValueKind.Undefined && element.TryGetProperty(name, out var value) ? value.GetString() ?? string.Empty : string.Empty;

        static string DescribeException(JsonElement details)
        {
            if (details.ValueKind == JsonValueKind.Undefined) return "JavaScript exception";

            var text = ReadString(details, "text");
            var url = ReadString(details, "url");
            var line = details.TryGetProperty("lineNumber", out var lineProp) && lineProp.TryGetInt32(out var lineNumber) ? lineNumber + 1 : 0;
            var column = details.TryGetProperty("columnNumber", out var columnProp) && columnProp.TryGetInt32(out var columnNumber) ? columnNumber + 1 : 0;
            var description = string.Empty;

            if (details.TryGetProperty("exception", out var exception))
            {
                description = ReadString(exception, "description");
                if (string.IsNullOrWhiteSpace(description)) description = ReadString(exception, "value");
                if (string.IsNullOrWhiteSpace(description)) description = ReadString(exception, "className");
            }

            if (string.IsNullOrWhiteSpace(description) && details.TryGetProperty("stackTrace", out var stack) && stack.TryGetProperty("callFrames", out var frames) && frames.ValueKind == JsonValueKind.Array)
            {
                foreach (var frame in frames.EnumerateArray())
                {
                    var frameUrl = ReadString(frame, "url");
                    var functionName = ReadString(frame, "functionName");
                    var frameLine = frame.TryGetProperty("lineNumber", out var frameLineProp) && frameLineProp.TryGetInt32(out var frameLineNumber) ? frameLineNumber + 1 : 0;
                    if (!string.IsNullOrWhiteSpace(frameUrl) || !string.IsNullOrWhiteSpace(functionName))
                    {
                        description = (string.IsNullOrWhiteSpace(functionName) ? "anonymous" : functionName) + (string.IsNullOrWhiteSpace(frameUrl) ? string.Empty : " @ " + frameUrl) + (frameLine > 0 ? ":" + frameLine : string.Empty);
                        break;
                    }
                }
            }

            var where = string.IsNullOrWhiteSpace(url) ? string.Empty : " @ " + url + (line > 0 ? ":" + line + (column > 0 ? ":" + column : string.Empty) : string.Empty);
            var message = (string.IsNullOrWhiteSpace(text) ? "JavaScript exception" : text) + (string.IsNullOrWhiteSpace(description) ? string.Empty : " - " + description) + where;
            return message.Length > 1400 ? message[..1400] + "…" : message;
        }

        try
        {
            await _core.CallDevToolsProtocolMethodAsync("Runtime.enable", "{}").ConfigureAwait(true);
            await _core.CallDevToolsProtocolMethodAsync("Network.enable", "{}").ConfigureAwait(true);
            try { await _core.CallDevToolsProtocolMethodAsync("Log.enable", "{}").ConfigureAwait(true); } catch { }

            var exceptionReceiver = _core.GetDevToolsProtocolEventReceiver("Runtime.exceptionThrown");
            exceptionReceiver.DevToolsProtocolEventReceived += (sender, e) =>
            {
                try
                {
                    if (!IsProtectedMediaUrl(_core?.Source)) return;
                    using var doc = ParseDevToolsJson(e.ParameterObjectAsJson);
                    var root = doc.RootElement;
                    JsonElement details;
                    var hasDetails = root.TryGetProperty("exceptionDetails", out details);
                    SendProtectedMediaDiagnostic("Protected media page JavaScript exception: " + (hasDetails ? DescribeException(details) : "JavaScript exception"));
                }
                catch { }
            };

            var failedReceiver = _core.GetDevToolsProtocolEventReceiver("Network.loadingFailed");
            failedReceiver.DevToolsProtocolEventReceived += (sender, e) =>
            {
                try
                {
                    if (!IsProtectedMediaUrl(_core?.Source)) return;
                    using var doc = ParseDevToolsJson(e.ParameterObjectAsJson);
                    var root = doc.RootElement;
                    var error = root.TryGetProperty("errorText", out var errorProp) ? errorProp.GetString() : "unknown";
                    var type = root.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : "resource";
                    var blocked = root.TryGetProperty("blockedReason", out var blockedProp) ? blockedProp.GetString() : null;
                    var requestId = ReadString(root, "requestId");
                    if (!string.IsNullOrWhiteSpace(requestId) && _primeResourceDiagnosticsByRequestId.TryRemove(requestId, out var info))
                        SendPrimeResourceDiagnostic("Prime/Amazon player load failed: " + type + " " + error + (string.IsNullOrWhiteSpace(blocked) ? string.Empty : " blocked=" + blocked) + " " + ShortUrl(info.Url));
                    SendProtectedMediaDiagnostic("Protected media network load failed: " + type + " " + error + (string.IsNullOrWhiteSpace(blocked) ? string.Empty : " blocked=" + blocked));
                }
                catch { }
            };

            var finishedReceiver = _core.GetDevToolsProtocolEventReceiver("Network.loadingFinished");
            finishedReceiver.DevToolsProtocolEventReceived += (sender, e) =>
            {
                try
                {
                    if (!IsProtectedMediaUrl(_core?.Source)) return;
                    using var doc = ParseDevToolsJson(e.ParameterObjectAsJson);
                    var root = doc.RootElement;
                    var requestId = ReadString(root, "requestId");
                    if (string.IsNullOrWhiteSpace(requestId) || !_primeResourceDiagnosticsByRequestId.TryRemove(requestId, out var info)) return;
                    var encoded = root.TryGetProperty("encodedDataLength", out var encodedProp) && encodedProp.TryGetDouble(out var bytes) ? bytes : 0;
                    SendPrimeResourceDiagnostic("Prime/Amazon player load finished: " + ShortUrl(info.Url) + " | encodedBytes=" + encoded.ToString("0", CultureInfo.InvariantCulture));
                }
                catch { }
            };

            var requestReceiver = _core.GetDevToolsProtocolEventReceiver("Network.requestWillBeSent");
            requestReceiver.DevToolsProtocolEventReceived += (sender, e) =>
            {
                try
                {
                    if (!IsProtectedMediaUrl(_core?.Source)) return;
                    using var doc = ParseDevToolsJson(e.ParameterObjectAsJson);
                    var root = doc.RootElement;
                    var hasRedirect = root.TryGetProperty("redirectResponse", out JsonElement redirectResponse);
                    var type = root.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : string.Empty;
                    var requestId = ReadString(root, "requestId");
                    var documentUrl = ReadString(root, "documentURL");
                    var url = root.TryGetProperty("request", out var req) && req.TryGetProperty("url", out var urlProp) ? urlProp.GetString() : null;

                    if (!string.IsNullOrWhiteSpace(url) && IsPrimePlayerDiagnosticResourceUrl(url) && !string.IsNullOrWhiteSpace(requestId))
                    {
                        var info = new PrimeResourceRequestDiagnostic
                        {
                            Url = url,
                            Type = type,
                            DocumentUrl = documentUrl,
                            Initiator = root.TryGetProperty("initiator", out var initiator) ? DescribePrimeRequestInitiator(initiator) : string.Empty
                        };
                        _primeResourceDiagnosticsByRequestId[requestId] = info;
                        var headerText = req.ValueKind != JsonValueKind.Undefined && req.TryGetProperty("headers", out var headers) ? FormatSafeDiagnosticHeaders(headers, requestHeaders: true) : "headers unavailable";
                        SendPrimeResourceDiagnostic("Prime/Amazon player request: " + type + " " + ShortUrl(url) + " | document=" + ShortUrl(documentUrl) + " | initiator=" + info.Initiator + " | " + headerText);
                    }

                    if (!hasRedirect && !string.Equals(type, "Document", StringComparison.OrdinalIgnoreCase)) return;
                    if (!string.IsNullOrWhiteSpace(url))
                        SendProtectedMediaDiagnostic((hasRedirect ? "Protected media redirect: " : "Protected media document request: ") + url);
                }
                catch { }
            };

            var requestExtraReceiver = _core.GetDevToolsProtocolEventReceiver("Network.requestWillBeSentExtraInfo");
            requestExtraReceiver.DevToolsProtocolEventReceived += (sender, e) =>
            {
                try
                {
                    if (!IsProtectedMediaUrl(_core?.Source)) return;
                    using var doc = ParseDevToolsJson(e.ParameterObjectAsJson);
                    var root = doc.RootElement;
                    var requestId = ReadString(root, "requestId");
                    if (string.IsNullOrWhiteSpace(requestId) || !_primeResourceDiagnosticsByRequestId.TryGetValue(requestId, out var info)) return;

                    var headerText = root.TryGetProperty("headers", out var headers) ? FormatSafeDiagnosticHeaders(headers, requestHeaders: true) : "headers unavailable";
                    var cookieText = DescribePrimeCookieMetadata(root, "associatedCookies", includeZero: true) + DescribePrimeCookieMetadata(root, "blockedCookies", includeZero: true);
                    SendPrimeResourceDiagnostic("Prime/Amazon player request extra: " + ShortUrl(info.Url) + " | " + headerText + cookieText);
                }
                catch { }
            };

            var responseReceiver = _core.GetDevToolsProtocolEventReceiver("Network.responseReceived");
            responseReceiver.DevToolsProtocolEventReceived += (sender, e) =>
            {
                try
                {
                    if (!IsProtectedMediaUrl(_core?.Source)) return;
                    using var doc = ParseDevToolsJson(e.ParameterObjectAsJson);
                    var root = doc.RootElement;
                    var requestId = ReadString(root, "requestId");
                    if (!root.TryGetProperty("response", out var response)) return;
                    var url = ReadString(response, "url");
                    if (!IsPrimePlayerDiagnosticResourceUrl(url) && (string.IsNullOrWhiteSpace(requestId) || !_primeResourceDiagnosticsByRequestId.ContainsKey(requestId))) return;

                    if (!string.IsNullOrWhiteSpace(requestId) && !_primeResourceDiagnosticsByRequestId.ContainsKey(requestId))
                    {
                        _primeResourceDiagnosticsByRequestId[requestId] = new PrimeResourceRequestDiagnostic
                        {
                            Url = url,
                            Type = ReadString(root, "type"),
                            DocumentUrl = string.Empty,
                            Initiator = string.Empty
                        };
                    }

                    var status = response.TryGetProperty("status", out var statusProp) && statusProp.TryGetInt32(out var statusCode) ? statusCode : 0;
                    var statusText = ReadString(response, "statusText");
                    var mime = ReadString(response, "mimeType");
                    var protocol = ReadString(response, "protocol");
                    var fromDisk = response.TryGetProperty("fromDiskCache", out var diskProp) && diskProp.ValueKind == JsonValueKind.True;
                    var fromService = response.TryGetProperty("fromServiceWorker", out var serviceProp) && serviceProp.ValueKind == JsonValueKind.True;
                    var headerText = response.TryGetProperty("headers", out var headers) ? FormatSafeDiagnosticHeaders(headers, requestHeaders: false) : "headers unavailable";
                    SendPrimeResourceDiagnostic("Prime/Amazon player response: HTTP " + status + (string.IsNullOrWhiteSpace(statusText) ? string.Empty : " " + statusText) + " " + ShortUrl(url) + " | type=" + ReadString(root, "type") + " mime=" + mime + " protocol=" + protocol + " diskCache=" + fromDisk + " serviceWorker=" + fromService + " | " + headerText);
                    if (status == 403 && !string.IsNullOrWhiteSpace(requestId) && IsPrimePlayerDiagnosticResourceUrl(url))
                        _ = LogPrimeResponseBodyAsync(requestId, url);
                }
                catch { }
            };

            var responseExtraReceiver = _core.GetDevToolsProtocolEventReceiver("Network.responseReceivedExtraInfo");
            responseExtraReceiver.DevToolsProtocolEventReceived += (sender, e) =>
            {
                try
                {
                    if (!IsProtectedMediaUrl(_core?.Source)) return;
                    using var doc = ParseDevToolsJson(e.ParameterObjectAsJson);
                    var root = doc.RootElement;
                    var requestId = ReadString(root, "requestId");
                    if (string.IsNullOrWhiteSpace(requestId) || !_primeResourceDiagnosticsByRequestId.TryGetValue(requestId, out var info)) return;
                    var status = root.TryGetProperty("statusCode", out var statusProp) && statusProp.TryGetInt32(out var statusCode) ? statusCode : 0;
                    var headerText = root.TryGetProperty("headers", out var headers) ? FormatSafeDiagnosticHeaders(headers, requestHeaders: false) : "headers unavailable";
                    var setCookieText = root.TryGetProperty("headers", out var setCookieHeaders) ? DescribePrimeSetCookieMetadata(setCookieHeaders) : string.Empty;
                    var blockedCookieText = DescribePrimeCookieMetadata(root, "blockedCookies", includeZero: status == 403);
                    SendPrimeResourceDiagnostic("Prime/Amazon player response extra: HTTP " + status + " " + ShortUrl(info.Url) + " | " + headerText + setCookieText + blockedCookieText);
                }
                catch { }
            };

            var logReceiver = _core.GetDevToolsProtocolEventReceiver("Log.entryAdded");
            logReceiver.DevToolsProtocolEventReceived += (sender, e) =>
            {
                try
                {
                    if (!IsProtectedMediaUrl(_core?.Source)) return;
                    using var doc = ParseDevToolsJson(e.ParameterObjectAsJson);
                    var root = doc.RootElement;
                    if (!root.TryGetProperty("entry", out var entry)) return;
                    var level = ReadString(entry, "level");
                    var text = ReadString(entry, "text");
                    var url = ReadString(entry, "url");
                    if (string.IsNullOrWhiteSpace(text)) return;
                    SendProtectedMediaDiagnostic("Protected media browser log " + (string.IsNullOrWhiteSpace(level) ? "entry" : level) + ": " + text + (string.IsNullOrWhiteSpace(url) ? string.Empty : " @ " + url));
                }
                catch { }
            };

            _protectedMediaDiagnosticsInstalled = true;
            Send(new { op = "status", text = "Protected media diagnostics active (redirect/network/JS error logging + passive Prime/AIV headers/cookies/403 body)" });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Protected media diagnostics could not be enabled: " + ex.Message });
        }
    }

    private static async Task LogPrimeResponseBodyAsync(string requestId, string url)
    {
        try
        {
            if (_core is null || string.IsNullOrWhiteSpace(requestId)) return;
            if (!_primeResponseBodyLoggedByRequestId.TryAdd(requestId, 0)) return;

            // CloudFront/S3 403s often return a tiny XML body with the real reason
            // (AccessDenied, MissingKey, SignatureDoesNotMatch, expired signed cookie, etc.).
            // Wait briefly so the response body is available after responseReceived.
            await Task.Delay(200).ConfigureAwait(true);
            if (_core is null) return;

            var payload = JsonSerializer.Serialize(new { requestId });
            var raw = await _core.CallDevToolsProtocolMethodAsync("Network.getResponseBody", payload).ConfigureAwait(true);
            using var doc = JsonDocument.Parse(System.Text.Encoding.UTF8.GetBytes(raw ?? "{}"));
            var root = doc.RootElement;
            var body = root.TryGetProperty("body", out var bodyProp) ? bodyProp.GetString() ?? string.Empty : string.Empty;
            var base64 = root.TryGetProperty("base64Encoded", out var base64Prop) && base64Prop.ValueKind == JsonValueKind.True;

            if (base64 && !string.IsNullOrWhiteSpace(body))
            {
                try
                {
                    body = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(body));
                }
                catch
                {
                    body = "<base64 response body could not be decoded>";
                }
            }

            body = SanitisePrimeResponseBody(body);
            SendPrimeResourceDiagnostic("Prime/Amazon 403 response body: " + ShortUrl(url) + " | " + (string.IsNullOrWhiteSpace(body) ? "<empty>" : body));
        }
        catch (Exception ex)
        {
            SendPrimeResourceDiagnostic("Prime/Amazon 403 response body unavailable: " + ShortUrl(url) + " | " + ex.Message);
        }
    }

    private static async Task LogPrimeCookieJarSnapshotAsync(string? currentUrl)
    {
        try
        {
            if (_core is null || !IsProtectedMediaUrl(currentUrl)) return;
            if (IsEnvEnabled("RAVACAST_DISABLE_PRIME_COOKIE_JAR_DIAGNOSTICS")) return;

            // Run slightly after load so cookies written by the detail/salp bootstrap have
            // landed in the WebView2 profile before we inspect the jar. Values are never logged.
            await Task.Delay(750).ConfigureAwait(true);
            if (_core is null) return;

            var urls = new[]
            {
                "https://www.amazon.co.uk/",
                "https://www.amazon.co.uk/gp/video/",
                "https://js-assets.aiv-cdn.net/",
                "https://aiv-cdn.net/",
                "https://atv-ps.amazon.co.uk/",
                "https://www.primevideo.com/"
            };

            foreach (var url in urls)
            {
                try
                {
                    var cookies = await _core.CookieManager.GetCookiesAsync(url).ConfigureAwait(true);
                    SendPrimeResourceDiagnostic("Prime/Amazon cookie jar: " + ShortUrl(url) + " | " + DescribeCookieJar(cookies));
                }
                catch (Exception ex)
                {
                    SendPrimeResourceDiagnostic("Prime/Amazon cookie jar unavailable: " + ShortUrl(url) + " | " + ex.Message);
                }
            }
        }
        catch { }
    }

    private static string DescribeCookieJar(IReadOnlyList<CoreWebView2Cookie>? cookies)
    {
        try
        {
            if (cookies is null || cookies.Count == 0) return "cookies=0";

            var domains = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            var names = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            var sameSites = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            var secure = 0;

            foreach (var cookie in cookies)
            {
                if (!string.IsNullOrWhiteSpace(cookie.Name)) names.Add(cookie.Name);
                if (!string.IsNullOrWhiteSpace(cookie.Domain)) domains.Add(cookie.Domain);
                if (cookie.IsSecure) secure++;
                sameSites.Add(cookie.SameSite.ToString());
            }

            var parts = new List<string> { "cookies=" + cookies.Count };
            if (names.Count > 0) parts.Add("names=" + string.Join(",", names.Take(10)));
            if (domains.Count > 0) parts.Add("domains=" + string.Join(",", domains.Take(8)));
            if (sameSites.Count > 0) parts.Add("sameSite=" + string.Join(",", sameSites.Take(4)));
            if (secure > 0) parts.Add("secure=" + secure);
            return string.Join(" | ", parts);
        }
        catch { return "cookies=<describe failed>"; }
    }

    private static string SanitisePrimeResponseBody(string body)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(body)) return string.Empty;
            body = body.Replace("\r", " ", StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal).Replace("\t", " ", StringComparison.Ordinal).Trim();
            while (body.Contains("  ", StringComparison.Ordinal)) body = body.Replace("  ", " ", StringComparison.Ordinal);
            if (body.Length > 1200) body = body[..1200] + "…";
            return body;
        }
        catch { return "<response body sanitise failed>"; }
    }

    private static void SendProtectedMediaDiagnostic(string message)
    {
        var now = Environment.TickCount64;
        if (now - Interlocked.Read(ref _lastProtectedMediaDiagnosticTick) < 500) return;
        Interlocked.Exchange(ref _lastProtectedMediaDiagnosticTick, now);
        Send(new { op = "status", text = message });
    }

    private static void SendPrimeResourceDiagnostic(string message)
    {
        try
        {
            if (IsEnvEnabled("RAVACAST_DISABLE_PRIME_RESOURCE_DIAGNOSTICS")) return;
            var count = Interlocked.Increment(ref _primeResourceDiagnosticCount);
            if (count > 120 && !message.Contains("WebLoader.js", StringComparison.OrdinalIgnoreCase)) return;
            if (message.Length > 1900) message = message[..1900] + "…";
            Send(new { op = "status", text = message });
        }
        catch { }
    }

    private static bool IsPrimePlayerDiagnosticResourceUrl(string? rawUri)
    {
        if (string.IsNullOrWhiteSpace(rawUri)) return false;
        if (!Uri.TryCreate(rawUri, UriKind.Absolute, out var uri)) return false;
        var host = uri.Host.ToLowerInvariant();
        var path = uri.AbsolutePath.ToLowerInvariant();

        // Keep the original AIV player-script diagnostics, but also track the Prime
        // session bootstrap documents that produce/consume the salp token. If Amazon
        // is minting a signed/session context before WebLoader.js, these two document
        // requests are where we should see associated cookies or Set-Cookie metadata.
        return host.Equals("aiv-cdn.net", StringComparison.OrdinalIgnoreCase)
            || host.EndsWith(".aiv-cdn.net", StringComparison.OrdinalIgnoreCase)
            || path.Contains("/playback/web_player/", StringComparison.OrdinalIgnoreCase)
            || path.Contains("webloader.js", StringComparison.OrdinalIgnoreCase)
            || ((host.Equals("www.amazon.co.uk", StringComparison.OrdinalIgnoreCase) || host.EndsWith(".amazon.co.uk", StringComparison.OrdinalIgnoreCase))
                && (path.Equals("/gp/video/salp/i", StringComparison.OrdinalIgnoreCase)
                    || path.StartsWith("/gp/video/detail/", StringComparison.OrdinalIgnoreCase)));
    }

    private static string ShortUrl(string? rawUrl)
    {
        if (string.IsNullOrWhiteSpace(rawUrl)) return "<none>";
        try
        {
            if (Uri.TryCreate(rawUrl, UriKind.Absolute, out var uri))
            {
                var text = uri.Host + uri.AbsolutePath;
                if (!string.IsNullOrWhiteSpace(uri.Query)) text += "?…";
                return text.Length > 220 ? text[..220] + "…" : text;
            }
        }
        catch { }

        return rawUrl.Length > 220 ? rawUrl[..220] + "…" : rawUrl;
    }

    private static string DescribePrimeRequestInitiator(JsonElement initiator)
    {
        try
        {
            var type = initiator.TryGetProperty("type", out var typeProp) ? typeProp.GetString() ?? string.Empty : string.Empty;
            var url = initiator.TryGetProperty("url", out var urlProp) ? urlProp.GetString() ?? string.Empty : string.Empty;
            var line = initiator.TryGetProperty("lineNumber", out var lineProp) && lineProp.TryGetInt32(out var lineNumber) ? lineNumber + 1 : 0;
            if (string.IsNullOrWhiteSpace(url) && initiator.TryGetProperty("stack", out var stack) && stack.TryGetProperty("callFrames", out var frames) && frames.ValueKind == JsonValueKind.Array)
            {
                foreach (var frame in frames.EnumerateArray())
                {
                    if (frame.TryGetProperty("url", out var frameUrlProp))
                    {
                        url = frameUrlProp.GetString() ?? string.Empty;
                        line = frame.TryGetProperty("lineNumber", out var frameLineProp) && frameLineProp.TryGetInt32(out var frameLineNumber) ? frameLineNumber + 1 : 0;
                        if (!string.IsNullOrWhiteSpace(url)) break;
                    }
                }
            }

            var result = string.IsNullOrWhiteSpace(type) ? "unknown" : type;
            if (!string.IsNullOrWhiteSpace(url)) result += " @ " + ShortUrl(url) + (line > 0 ? ":" + line : string.Empty);
            return result;
        }
        catch { return "unknown"; }
    }

    private static string DescribePrimeCookieMetadata(JsonElement root, string propertyName, bool includeZero = false)
    {
        try
        {
            var label = propertyName.Equals("associatedCookies", StringComparison.OrdinalIgnoreCase) ? "associatedCookies" : "blockedCookies";
            if (!root.TryGetProperty(propertyName, out var array) || array.ValueKind != JsonValueKind.Array) return includeZero ? " | " + label + "=0/missing" : string.Empty;
            var total = 0;
            var blocked = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in array.EnumerateArray())
            {
                total++;
                if (item.TryGetProperty("blockedReasons", out var reasons) && reasons.ValueKind == JsonValueKind.Array)
                {
                    foreach (var reason in reasons.EnumerateArray())
                    {
                        var text = reason.GetString();
                        if (!string.IsNullOrWhiteSpace(text)) blocked.Add(text);
                    }
                }
                if (item.TryGetProperty("blockedReason", out var reasonProp))
                {
                    var text = reasonProp.GetString();
                    if (!string.IsNullOrWhiteSpace(text)) blocked.Add(text);
                }
            }

            if (total <= 0) return includeZero ? " | " + label + "=0" : string.Empty;
            return " | " + label + "=" + total + (blocked.Count == 0 ? string.Empty : " reasons=" + string.Join(",", blocked.Take(8)));
        }
        catch { return string.Empty; }
    }

    private static string DescribePrimeSetCookieMetadata(JsonElement headers)
    {
        try
        {
            if (headers.ValueKind != JsonValueKind.Object) return string.Empty;

            var cookieNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            var domains = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            var sameSites = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            var secureCount = 0;
            var total = 0;

            foreach (var property in headers.EnumerateObject())
            {
                if (!property.Name.Equals("set-cookie", StringComparison.OrdinalIgnoreCase) && !property.Name.Equals("set-cookie2", StringComparison.OrdinalIgnoreCase)) continue;

                var raw = property.Value.ValueKind == JsonValueKind.String ? property.Value.GetString() ?? string.Empty : property.Value.ToString();
                foreach (var line in SplitSetCookieHeaderLines(raw))
                {
                    var trimmed = line.Trim();
                    if (string.IsNullOrWhiteSpace(trimmed)) continue;
                    total++;

                    var firstSemi = trimmed.IndexOf(';');
                    var namePart = firstSemi >= 0 ? trimmed[..firstSemi] : trimmed;
                    var equals = namePart.IndexOf('=');
                    if (equals > 0) cookieNames.Add(namePart[..equals].Trim());

                    foreach (var segment in trimmed.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                    {
                        if (segment.Equals("secure", StringComparison.OrdinalIgnoreCase))
                        {
                            secureCount++;
                            continue;
                        }

                        if (segment.StartsWith("domain=", StringComparison.OrdinalIgnoreCase))
                            domains.Add(segment[7..].Trim());
                        else if (segment.StartsWith("samesite=", StringComparison.OrdinalIgnoreCase))
                            sameSites.Add(segment[9..].Trim());
                    }
                }
            }

            if (total == 0) return string.Empty;

            var parts = new List<string> { "setCookie=" + total };
            if (cookieNames.Count > 0) parts.Add("names=" + string.Join(",", cookieNames.Take(8)));
            if (domains.Count > 0) parts.Add("domains=" + string.Join(",", domains.Take(6)));
            if (sameSites.Count > 0) parts.Add("sameSite=" + string.Join(",", sameSites.Take(4)));
            if (secureCount > 0) parts.Add("secure=" + secureCount);
            return " | " + string.Join(" ", parts);
        }
        catch { return string.Empty; }
    }

    private static IEnumerable<string> SplitSetCookieHeaderLines(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) yield break;

        // CDP may hand us one newline-separated Set-Cookie string, or a folded value. Do
        // not attempt to parse values deeply; this is only metadata so secrets stay out of logs.
        foreach (var line in raw.Replace("\r", "\n", StringComparison.Ordinal).Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            yield return line;
    }

    private static string FormatSafeDiagnosticHeaders(JsonElement headers, bool requestHeaders)
    {
        try
        {
            if (headers.ValueKind != JsonValueKind.Object) return "headers unavailable";
            var parts = new List<string>();
            var redactedSensitive = 0;
            foreach (var property in headers.EnumerateObject())
            {
                var name = property.Name;
                if (IsSensitiveDiagnosticHeader(name))
                {
                    redactedSensitive++;
                    continue;
                }

                if (!IsUsefulDiagnosticHeader(name, requestHeaders)) continue;
                var value = property.Value.ValueKind == JsonValueKind.String ? property.Value.GetString() ?? string.Empty : property.Value.ToString();
                value = value.Replace("\r", " ", StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal).Trim();
                if (value.Length > 220) value = value[..220] + "…";
                parts.Add(name + "=" + value);
            }

            if (redactedSensitive > 0) parts.Add("sensitiveHeadersRedacted=" + redactedSensitive);
            return parts.Count == 0 ? "safe headers unavailable" : string.Join("; ", parts.Take(18));
        }
        catch { return "headers unavailable"; }
    }

    private static bool IsSensitiveDiagnosticHeader(string name)
    {
        var n = name.Trim().ToLowerInvariant();
        return n.Contains("cookie", StringComparison.Ordinal)
            || n.Contains("authorization", StringComparison.Ordinal)
            || n.Contains("token", StringComparison.Ordinal)
            || n.Contains("credential", StringComparison.Ordinal)
            || n.Contains("secret", StringComparison.Ordinal)
            || n.Equals("x-api-key", StringComparison.Ordinal)
            || n.Equals("x-amz-security-token", StringComparison.Ordinal);
    }

    private static bool IsUsefulDiagnosticHeader(string name, bool requestHeaders)
    {
        var n = name.Trim().ToLowerInvariant();
        if (requestHeaders)
        {
            return n is "accept" or "accept-language" or "cache-control" or "pragma" or "priority" or "referer" or "origin" or "user-agent" or "range" or "if-none-match" or "if-modified-since" or "x-requested-with"
                || n.StartsWith("sec-ch-", StringComparison.Ordinal)
                || n.StartsWith("sec-fetch-", StringComparison.Ordinal);
        }

        return n is "access-control-allow-origin" or "access-control-allow-credentials" or "age" or "cache-control" or "content-type" or "date" or "etag" or "expires" or "last-modified" or "server" or "status" or "vary" or "via" or "x-cache" or "x-amz-cf-id" or "x-amz-cf-pop" or "x-amz-error-code" or "x-amz-error-message";
    }

    private static async Task ApplyDesktopEdgePersonaAsync(string? browserVersionString)
    {
        if (_core is null) return;
        if (!UseDesktopEdgeUserAgentOverride() || IsEnvEnabled("RAVACAST_WEBVIEW2_STOCK_UA") || IsEnvEnabled("RAVACAST_WEBVIEW2_DISABLE_DESKTOP_EDGE_UA"))
        {
            Send(new { op = "status", text = "WebView2 desktop Edge user-agent override disabled" });
            return;
        }

        var userAgent = BuildDesktopEdgeUserAgent(browserVersionString);
        var fullVersion = ExtractEdgeBrowserVersion(browserVersionString);
        var majorVersion = BuildDesktopEdgeMajorVersion(browserVersionString);
        _desktopEdgeUserAgent = userAgent;
        _desktopEdgeMajorVersion = majorVersion;

        try
        {
            _core.Settings.UserAgent = userAgent;
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 user-agent override failed: " + ex.Message });
        }

        if (IsEnvEnabled("RAVACAST_WEBVIEW2_FULL_DESKTOP_EDGE_PERSONA"))
        {
            InstallDesktopEdgePersonaRequestShim();
            await InstallDesktopEdgePersonaScriptAsync(userAgent, fullVersion, majorVersion).ConfigureAwait(true);
            Send(new { op = "status", text = "Protected media compatibility: full desktop Edge persona active", detail = userAgent });
        }
        else
        {
            Send(new { op = "status", text = "Protected media compatibility: desktop Edge user-agent active (UA only; no JS/UA-CH spoof)", detail = userAgent });
        }
    }

    private static string BuildDesktopEdgeUserAgent(string? browserVersionString)
    {
        var version = ExtractEdgeBrowserVersion(browserVersionString);
        return $"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36 Edg/{version}";
    }

    private static string BuildDesktopEdgeMajorVersion(string? browserVersionString)
    {
        var version = ExtractEdgeBrowserVersion(browserVersionString);
        var dot = version.IndexOf('.');
        return dot > 0 ? version[..dot] : version;
    }

    private static string ExtractEdgeBrowserVersion(string? browserVersionString)
    {
        try
        {
            var raw = browserVersionString ?? string.Empty;
            var slash = raw.IndexOf('/');
            if (slash >= 0 && slash + 1 < raw.Length)
                raw = raw[(slash + 1)..];

            var token = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(token))
            {
                var cleaned = new string(token.Where(c => char.IsDigit(c) || c == '.').ToArray()).Trim('.');
                if (!string.IsNullOrWhiteSpace(cleaned) && cleaned.Any(char.IsDigit))
                    return cleaned;
            }
        }
        catch { }

        return "128.0.0.0";
    }



    private static async Task ApplyPrimeAmazonBrowserPersonaAsync(string? browserVersionString)
    {
        if (_core is null || _primeAmazonBrowserPersonaInstalled) return;
        if (!UsePrimeAmazonBrowserPersona())
        {
            Send(new { op = "status", text = "Global browser persona disabled" });
            return;
        }

        var userAgent = BuildDesktopEdgeUserAgent(browserVersionString);
        var fullVersion = ExtractEdgeBrowserVersion(browserVersionString);
        var majorVersion = BuildDesktopEdgeMajorVersion(browserVersionString);

        _primeAmazonBrowserPersonaUserAgent = userAgent;
        _primeAmazonBrowserPersonaFullVersion = fullVersion;
        _primeAmazonBrowserPersonaMajorVersion = majorVersion;
        _desktopEdgeUserAgent ??= userAgent;
        _desktopEdgeMajorVersion ??= majorVersion;

        try
        {
            // Setting UserAgent alone is not enough: WebView2 still advertises itself through
            // Sec-CH-UA as "Microsoft Edge WebView2". CDP userAgentMetadata is the part
            // that drives Sec-CH-UA-* and navigator.userAgentData, so mask it globally.
            _core.Settings.UserAgent = userAgent;
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Global browser persona User-Agent override failed: " + ex.Message });
        }

        await ApplyPrimeAmazonDevToolsUserAgentOverrideAsync(userAgent, fullVersion, majorVersion).ConfigureAwait(true);
        InstallPrimeAmazonBrowserPersonaRequestShim();
        await InstallDesktopEdgePersonaScriptAsync(userAgent, fullVersion, majorVersion).ConfigureAwait(true);

        _primeAmazonBrowserPersonaInstalled = true;
        Send(new { op = "status", text = "Global browser persona active: WebView2 brand removed from UA/client hints before navigation", detail = userAgent });
    }

    private static object[] BuildBrowserPersonaBrandList(string majorVersion)
        => new object[]
        {
            new { brand = "Not/A)Brand", version = "99" },
            new { brand = "Microsoft Edge", version = majorVersion },
            new { brand = "Chromium", version = majorVersion }
        };

    private static object[] BuildBrowserPersonaFullVersionList(string fullVersion)
        => new object[]
        {
            new { brand = "Not/A)Brand", version = "99.0.0.0" },
            new { brand = "Microsoft Edge", version = fullVersion },
            new { brand = "Chromium", version = fullVersion }
        };

    private static string BuildBrowserPersonaSecChUa(string majorVersion)
        => $"\"Not/A)Brand\";v=\"99\", \"Microsoft Edge\";v=\"{majorVersion}\", \"Chromium\";v=\"{majorVersion}\"";

    private static string BuildBrowserPersonaSecChUaFullVersionList(string fullVersion)
        => $"\"Not/A)Brand\";v=\"99.0.0.0\", \"Microsoft Edge\";v=\"{fullVersion}\", \"Chromium\";v=\"{fullVersion}\"";

    private static async Task ApplyPrimeAmazonDevToolsUserAgentOverrideAsync(string userAgent, string fullVersion, string majorVersion)
    {
        if (_core is null) return;
        try
        {
            await _core.CallDevToolsProtocolMethodAsync("Network.enable", "{}").ConfigureAwait(true);
            // Feed CDP plain language tags only, then force a clean outbound header in the
            // request shim. Passing q-values here can produce malformed duplicates such as
            // q=0.9;q=0.9 on some WebView2/Chromium runtimes.
            var payload = JsonSerializer.Serialize(new
            {
                userAgent,
                acceptLanguage = BrowserPersonaAcceptLanguageTags,
                platform = "Windows",
                userAgentMetadata = new
                {
                    brands = BuildBrowserPersonaBrandList(majorVersion),
                    fullVersionList = BuildBrowserPersonaFullVersionList(fullVersion),
                    fullVersion,
                    platform = "Windows",
                    platformVersion = "10.0.0",
                    architecture = "x86",
                    model = string.Empty,
                    mobile = false,
                    bitness = "64",
                    wow64 = false
                }
            });

            await _core.CallDevToolsProtocolMethodAsync("Network.setUserAgentOverride", payload).ConfigureAwait(true);
            Send(new { op = "status", text = "Global CDP user-agent metadata override active; WebView2 removed and clean language tags applied" });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Global CDP user-agent metadata override failed: " + ex.Message });
        }
    }

    private static void InstallPrimeAmazonBrowserPersonaRequestShim()
    {
        if (_core is null || _primeAmazonBrowserPersonaRequestShimInstalled) return;
        try
        {
            _core.AddWebResourceRequestedFilter("*", CoreWebView2WebResourceContext.All);
            _core.WebResourceRequested += OnPrimeAmazonBrowserPersonaWebResourceRequested;
            _primeAmazonBrowserPersonaRequestShimInstalled = true;
            Send(new { op = "status", text = "Global request-header safety shim active for all browser requests" });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Global request-header safety shim failed: " + ex.Message });
        }
    }

    private static void OnPrimeAmazonBrowserPersonaWebResourceRequested(object? sender, CoreWebView2WebResourceRequestedEventArgs e)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(_primeAmazonBrowserPersonaUserAgent)) return;

            var major = string.IsNullOrWhiteSpace(_primeAmazonBrowserPersonaMajorVersion) ? "128" : _primeAmazonBrowserPersonaMajorVersion;
            var full = string.IsNullOrWhiteSpace(_primeAmazonBrowserPersonaFullVersion) ? major + ".0.0.0" : _primeAmazonBrowserPersonaFullVersion;
            var safeUa = BuildBrowserPersonaSecChUa(major);
            var safeFullUa = BuildBrowserPersonaSecChUaFullVersionList(full);

            TrySetWebResourceRequestHeader(e.Request.Headers, "User-Agent", _primeAmazonBrowserPersonaUserAgent);
            TrySetWebResourceRequestHeader(e.Request.Headers, "Accept-Language", BrowserPersonaAcceptLanguageHeader);
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA", safeUa);
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Mobile", "?0");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Platform", "\"Windows\"");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Full-Version-List", safeFullUa);
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Full-Version", "\"" + full + "\"");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Arch", "\"x86\"");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Bitness", "\"64\"");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Model", "\"\"");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Platform-Version", "\"10.0.0\"");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-WoW64", "?0");

            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastPrimeBrowserPersonaRequestLogTick) >= 5000)
            {
                Interlocked.Exchange(ref _lastPrimeBrowserPersonaRequestLogTick, now);
                Send(new { op = "status", text = "Global request headers normalised: WebView2 removed and Accept-Language cleaned", detail = ShortUrl(e.Request.Uri) });
            }
        }
        catch (Exception ex)
        {
            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastPrimeBrowserPersonaErrorTick) >= 5000)
            {
                Interlocked.Exchange(ref _lastPrimeBrowserPersonaErrorTick, now);
                Send(new { op = "error", message = "Global request-header safety shim failed: " + ex.Message });
            }
        }
    }

    private static bool IsPrimeAmazonBrowserPersonaUrl(string? rawUri)
    {
        if (!Uri.TryCreate(rawUri, UriKind.Absolute, out var uri)) return false;
        var host = uri.Host.ToLowerInvariant();
        return host.Contains(".amazon.", StringComparison.Ordinal)
            || host.StartsWith("amazon.", StringComparison.Ordinal)
            || host.EndsWith("primevideo.com", StringComparison.Ordinal)
            || host.EndsWith("amazonvideo.com", StringComparison.Ordinal)
            || host.EndsWith("media-amazon.com", StringComparison.Ordinal)
            || host.EndsWith("aiv-cdn.net", StringComparison.Ordinal);
    }

    private static void InstallPrimeWebView2ClientHintSanitizer()
    {
        if (_core is null || _primeWebView2ClientHintSanitizerInstalled) return;
        if (!UsePrimeClientHintSanitizer() || IsEnvEnabled("RAVACAST_DISABLE_PRIME_WEBVIEW2_CH_SANITIZER"))
        {
            Send(new { op = "status", text = "Prime/Amazon WebView2 client-hint sanitizer disabled" });
            return;
        }

        try
        {
            _core.AddWebResourceRequestedFilter("*://*.aiv-cdn.net/*", CoreWebView2WebResourceContext.All);
            _core.AddWebResourceRequestedFilter("*://aiv-cdn.net/*", CoreWebView2WebResourceContext.All);
            _core.WebResourceRequested += OnPrimeWebView2ClientHintSanitizerRequested;
            _primeWebView2ClientHintSanitizerInstalled = true;
            Send(new { op = "status", text = "Prime/Amazon WebView2 client-hint sanitizer active for AIV CDN player resources only" });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Prime/Amazon WebView2 client-hint sanitizer could not be enabled: " + ex.Message });
        }
    }

    private static void OnPrimeWebView2ClientHintSanitizerRequested(object? sender, CoreWebView2WebResourceRequestedEventArgs e)
    {
        try
        {
            if (!IsPrimePlayerDiagnosticResourceUrl(e.Request.Uri)) return;
            if (!_protectedMediaModeActive && !IsProtectedMediaUrl(_currentUrl) && !IsProtectedMediaUrl(_core?.Source)) return;

            var major = string.IsNullOrWhiteSpace(_desktopEdgeMajorVersion) ? "128" : _desktopEdgeMajorVersion;
            var safeUa = BuildBrowserPersonaSecChUa(major);

            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA", safeUa);
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Mobile", "?0");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Platform", "\"Windows\"");

            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastPrimeClientHintSanitizerLogTick) >= 5000)
            {
                Interlocked.Exchange(ref _lastPrimeClientHintSanitizerLogTick, now);
                Send(new { op = "status", text = "Prime/Amazon AIV CDN client hints sanitised: removed Microsoft Edge WebView2 brand from Sec-CH-UA", detail = ShortUrl(e.Request.Uri) });
            }
        }
        catch (Exception ex)
        {
            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastPrimeClientHintSanitizerErrorTick) >= 5000)
            {
                Interlocked.Exchange(ref _lastPrimeClientHintSanitizerErrorTick, now);
                Send(new { op = "error", message = "Prime/Amazon WebView2 client-hint sanitizer failed: " + ex.Message });
            }
        }
    }

    private static void InstallDesktopEdgePersonaRequestShim()
    {
        if (_core is null || _desktopEdgePersonaRequestShimInstalled) return;
        try
        {
            _core.AddWebResourceRequestedFilter("*", CoreWebView2WebResourceContext.All);
            _core.WebResourceRequested += OnDesktopEdgePersonaWebResourceRequested;
            _desktopEdgePersonaRequestShimInstalled = true;
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 desktop Edge request-header shim failed: " + ex.Message });
        }
    }

    private static void OnDesktopEdgePersonaWebResourceRequested(object? sender, CoreWebView2WebResourceRequestedEventArgs e)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(_desktopEdgeUserAgent)) return;
            if (!IsProtectedMediaUrl(e.Request.Uri) && !_protectedMediaModeActive) return;

            var major = string.IsNullOrWhiteSpace(_desktopEdgeMajorVersion) ? "128" : _desktopEdgeMajorVersion;
            TrySetWebResourceRequestHeader(e.Request.Headers, "User-Agent", _desktopEdgeUserAgent);
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA", $"\"Microsoft Edge\";v=\"{major}\", \"Chromium\";v=\"{major}\", \"Not=A?Brand\";v=\"24\"");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Mobile", "?0");
            TrySetWebResourceRequestHeader(e.Request.Headers, "Sec-CH-UA-Platform", "\"Windows\"");
        }
        catch (Exception ex)
        {
            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastDesktopEdgePersonaHeaderErrorTick) >= 5000)
            {
                Interlocked.Exchange(ref _lastDesktopEdgePersonaHeaderErrorTick, now);
                Send(new { op = "error", message = "WebView2 desktop Edge request-header shim failed: " + ex.Message });
            }
        }
    }

    private static void TrySetWebResourceRequestHeader(CoreWebView2HttpRequestHeaders headers, string name, string value)
    {
        try { headers.SetHeader(name, value); }
        catch { }
    }

    private static async Task InstallDesktopEdgePersonaScriptAsync(string userAgent, string fullVersion, string majorVersion)
    {
        if (_core is null || _desktopEdgePersonaScriptInstalled) return;
        try
        {
            var ua = EscapeJavaScriptSingleQuotedString(userAgent);
            var full = EscapeJavaScriptSingleQuotedString(fullVersion);
            var major = EscapeJavaScriptSingleQuotedString(majorVersion);
            var language = EscapeJavaScriptSingleQuotedString("en-GB");
            var script = $$"""
(() => {
    // Apply globally so pages never see the WebView2 browser brand via navigator.*.
    const ua = '{{ua}}';
    const fullVersion = '{{full}}';
    const majorVersion = '{{major}}';
    const language = '{{language}}';
    const languages = ['en-GB', 'en', 'en-US'];
    const brands = [
        { brand: 'Not/A)Brand', version: '99' },
        { brand: 'Microsoft Edge', version: majorVersion },
        { brand: 'Chromium', version: majorVersion }
    ];
    const fullVersionList = [
        { brand: 'Not/A)Brand', version: '99.0.0.0' },
        { brand: 'Microsoft Edge', version: fullVersion },
        { brand: 'Chromium', version: fullVersion }
    ];

    // WebView2 can still leave window.chrome.webview/host-object plumbing visible even when
    // web messages and host objects are disabled. Some sites probe it and trigger AccessDenied.
    // Shadow only the WebView2 branch while leaving the normal Chromium chrome object shape alive.
    try {
        const sourceChrome = (typeof window.chrome === 'object' && window.chrome) ? window.chrome : {};
        const cleanChrome = {};
        for (const key of Reflect.ownKeys(sourceChrome)) {
            if (key === 'webview') continue;
            try {
                const descriptor = Object.getOwnPropertyDescriptor(sourceChrome, key);
                Object.defineProperty(cleanChrome, key, descriptor || { value: sourceChrome[key], configurable: true, enumerable: true, writable: true });
            } catch { }
        }
        Object.defineProperty(window, 'chrome', { get: () => cleanChrome, configurable: true });
    } catch {
        try { Object.defineProperty(window.chrome || {}, 'webview', { get: () => undefined, configurable: true }); } catch { }
    }

    try { Object.defineProperty(Navigator.prototype, 'userAgent', { get: () => ua, configurable: true }); } catch { }
    try { Object.defineProperty(Navigator.prototype, 'appVersion', { get: () => ua.replace(/^Mozilla\//, ''), configurable: true }); } catch { }
    try { Object.defineProperty(Navigator.prototype, 'vendor', { get: () => 'Google Inc.', configurable: true }); } catch { }
    try { Object.defineProperty(Navigator.prototype, 'platform', { get: () => 'Win32', configurable: true }); } catch { }
    try { Object.defineProperty(Navigator.prototype, 'language', { get: () => language, configurable: true }); } catch { }
    try { Object.defineProperty(Navigator.prototype, 'languages', { get: () => languages.slice(), configurable: true }); } catch { }
    try {
        const userAgentData = {
            brands,
            mobile: false,
            platform: 'Windows',
            getHighEntropyValues: async (hints = []) => {
                const values = {
                    brands,
                    fullVersionList,
                    mobile: false,
                    platform: 'Windows',
                    architecture: 'x86',
                    bitness: '64',
                    model: '',
                    platformVersion: '10.0.0',
                    uaFullVersion: fullVersion
                };
                const result = { brands, mobile: false, platform: 'Windows' };
                for (const hint of hints) {
                    if (Object.prototype.hasOwnProperty.call(values, hint)) result[hint] = values[hint];
                }
                return result;
            },
            toJSON: () => ({ brands, mobile: false, platform: 'Windows' })
        };
        Object.defineProperty(Navigator.prototype, 'userAgentData', { get: () => userAgentData, configurable: true });
    } catch { }
})();
""";

            await _core.AddScriptToExecuteOnDocumentCreatedAsync(script).ConfigureAwait(true);
            _desktopEdgePersonaScriptInstalled = true;
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Global JavaScript browser persona failed: " + ex.Message });
        }
    }

    private static string EscapeJavaScriptSingleQuotedString(string value)
        => value.Replace("\\", "\\\\", StringComparison.Ordinal)
                .Replace("'", "\\'", StringComparison.Ordinal)
                .Replace("\r", "\\r", StringComparison.Ordinal)
                .Replace("\n", "\\n", StringComparison.Ordinal);

    private static void OnPermissionRequested(object? sender, CoreWebView2PermissionRequestedEventArgs e)
    {
        try
        {
            var uri = e.Uri;
            if (!IsProtectedMediaUrl(uri)) return;

            // Streaming services often use autoplay, fullscreen, clipboard and device capability
            // probes around their players. Let the embedded browser behave like a user-controlled
            // browser on allow-listed media services, without touching DRM/security enforcement.
            switch (e.PermissionKind)
            {
                case CoreWebView2PermissionKind.Microphone:
                case CoreWebView2PermissionKind.Camera:
                    return;
                default:
                    e.State = CoreWebView2PermissionState.Allow;
                    Send(new { op = "status", text = $"RavaCast allowed browser permission for protected media page: {e.PermissionKind}" });
                    break;
            }
        }
        catch { }
    }

    private static bool IsProtectedMediaUrl(string? rawUri)
    {
        if (string.IsNullOrWhiteSpace(rawUri)) return false;
        if (!Uri.TryCreate(rawUri, UriKind.Absolute, out var uri)) return false;
        if (!string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)) return false;

        var host = uri.Host.ToLowerInvariant();
        var full = rawUri.ToLowerInvariant();
        foreach (var fragment in ProtectedMediaHostFragments)
        {
            if (host.Equals(fragment, StringComparison.OrdinalIgnoreCase)
                || host.EndsWith("." + fragment, StringComparison.OrdinalIgnoreCase)
                || full.Contains(fragment, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static void OnWebMessageReceived(object? sender, CoreWebView2WebMessageReceivedEventArgs e)
    {
        try
        {
            using var doc = JsonDocument.Parse(e.WebMessageAsJson);
            var root = doc.RootElement;
            if (!root.TryGetProperty("source", out var source) || source.GetString() != "ravacast-eme-probe") return;
            var pageUrl = root.TryGetProperty("pageUrl", out var pageUrlProp) ? pageUrlProp.GetString() : null;
            var result = root.TryGetProperty("result", out var resultProp) ? resultProp.GetRawText() : root.GetRawText();
            Send(new { op = "status", text = "Protected media EME/CDM async probe for " + (pageUrl ?? "current page") + ": " + result });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Protected media EME/CDM async probe message failed: " + ex.Message });
        }
    }

    private static void ApplyProtectedMediaModeForUrl(string? rawUri)
        => _ = ApplyProtectedMediaModeForUrlAsync(rawUri);

    private static async Task ApplyProtectedMediaModeForUrlAsync(string? rawUri)
    {
        if (!UseProtectedMediaCompatibilityMode())
        {
            if (_protectedMediaModeActive)
            {
                _protectedMediaModeActive = false;
                Send(new { op = "status", text = "Protected media compatibility mode disabled; leaving extensions and normal WebView2 behaviour untouched" });
            }
            await Task.CompletedTask.ConfigureAwait(true);
            return;
        }

        var enabled = IsProtectedMediaUrl(rawUri);
        var changed = enabled != _protectedMediaModeActive;
        _protectedMediaModeActive = enabled;

        CoreWebView2BrowserExtension[] extensions;
        lock (_cleanBrowsingExtensionGate)
            extensions = [.. _cleanBrowsingExtensions];

        // Always enforce the desired extension state, even when the mode flag already
        // matches. A previous throttled switch could leave uBlock/Ghostery enabled while
        // navigating to Amazon/Prime, which can make the page sit black or never open.
        foreach (var extension in extensions)
        {
            try { await extension.EnableAsync(!enabled).ConfigureAwait(true); }
            catch (Exception ex) { Send(new { op = "error", message = $"RavaCast could not toggle browser extension '{extension.Name}': {ex.Message}" }); }
        }

        var now = Environment.TickCount64;
        var shouldReport = changed || now - Interlocked.Read(ref _lastProtectedMediaModeSwitchTick) >= 1500;
        if (!shouldReport) return;
        Interlocked.Exchange(ref _lastProtectedMediaModeSwitchTick, now);

        Send(new
        {
            op = "status",
            text = enabled
                ? "Protected media compatibility mode active: browser extensions and clean-browsing request blocks are paused for this streaming site"
                : "RavaCast clean-browsing blockers restored"
        });
    }

    private static async Task ProbeProtectedMediaSupportAsync(string? pageUrl)
    {
        if (_core is null) return;
        try
        {
            // Keep this probe host-side only. Enabling chrome.webview makes WebView2 obvious to
            // public pages, so the probe now returns through ExecuteScriptAsync instead of
            // window.chrome.webview.postMessage.
            const string script = """
(async () => {
    const result = {
        pageUrl: location.href,
        eme: !!navigator.requestMediaKeySystemAccess,
        chromeWebViewVisible: !!(window.chrome && window.chrome.webview),
        userAgent: navigator.userAgent,
        platform: navigator.platform,
        vendor: navigator.vendor,
        userAgentData: navigator.userAgentData ? {
            brands: navigator.userAgentData.brands,
            mobile: navigator.userAgentData.mobile,
            platform: navigator.userAgentData.platform
        } : null
    };
    const cfg = [{
        initDataTypes: ['cenc'],
        audioCapabilities: [{ contentType: 'audio/mp4; codecs="mp4a.40.2"' }],
        videoCapabilities: [{ contentType: 'video/mp4; codecs="avc1.640028"' }]
    }];
    async function test(name) {
        try {
            if (!navigator.requestMediaKeySystemAccess) return 'missing-eme';
            await navigator.requestMediaKeySystemAccess(name, cfg);
            return 'available';
        } catch (e) {
            return 'unavailable:' + ((e && e.name) ? e.name : String(e));
        }
    }
    result.widevine = await test('com.widevine.alpha');
    result.playready = await test('com.microsoft.playready');
    result.playreadyRecommendation = await test('com.microsoft.playready.recommendation');
    return result;
})()
""";

            var raw = await _core.ExecuteScriptAsync(script).ConfigureAwait(true);
            var result = raw;
            try
            {
                var decoded = JsonSerializer.Deserialize<string>(raw);
                if (!string.IsNullOrWhiteSpace(decoded)) result = decoded;
            }
            catch { }

            Send(new { op = "status", text = "Protected media EME/CDM probe for " + (pageUrl ?? "current page") + ": " + result });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Protected media EME/CDM probe failed: " + ex.Message });
        }
    }

    private static async Task ImportConsentCookiesAsync(JsonElement root)
    {
        try
        {
            var reload = root.TryGetProperty("reload", out var reloadProp) && reloadProp.GetBoolean();
            if (!root.TryGetProperty("cookies", out var cookiesProp)) return;
            var cookies = JsonSerializer.Deserialize<SharedConsentCookie[]>(cookiesProp.GetRawText()) ?? [];
            cookies = cookies.Where(IsShareableConsentCookie).Take(64).ToArray();
            if (_core is null)
            {
                _pendingImportConsentCookies = cookies;
                _pendingImportConsentReload = reload;
                return;
            }

            await AddConsentCookiesAsync(cookies, reload).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 consent cookie import failed: " + ex.Message });
        }
    }

    private static async Task ApplyPendingConsentCookiesAsync()
    {
        if (_pendingImportConsentCookies.Length == 0) return;
        var cookies = _pendingImportConsentCookies;
        var reload = _pendingImportConsentReload;
        _pendingImportConsentCookies = [];
        _pendingImportConsentReload = false;
        await AddConsentCookiesAsync(cookies, reload).ConfigureAwait(true);
    }

    private static async Task AddConsentCookiesAsync(IReadOnlyList<SharedConsentCookie> cookies, bool reload)
    {
        if (_core is null || cookies.Count == 0) return;
        foreach (var cookie in cookies.Where(IsShareableConsentCookie).Take(64))
        {
            try
            {
                var webCookie = _core.CookieManager.CreateCookie(cookie.Name, cookie.Value, cookie.Domain, string.IsNullOrWhiteSpace(cookie.Path) ? "/" : cookie.Path);
                webCookie.IsSecure = cookie.Secure;
                if (cookie.ExpiresUnixMs.HasValue && cookie.ExpiresUnixMs.Value > 0)
                    webCookie.Expires = DateTimeOffset.FromUnixTimeMilliseconds(cookie.ExpiresUnixMs.Value).UtcDateTime;
                webCookie.SameSite = ParseSameSite(cookie.SameSite);
                _core.CookieManager.AddOrUpdateCookie(webCookie);
            }
            catch { }
        }

        Send(new { op = "status", text = $"Imported {cookies.Count} shared consent cookies" });
        if (reload && _core is not null && !string.IsNullOrWhiteSpace(_core.Source) && !_core.Source.StartsWith("about:", StringComparison.OrdinalIgnoreCase))
        {
            await Task.Delay(100).ConfigureAwait(true);
            _core.Reload();
        }
    }

    private static void MarkConsentCookiesDirty()
    {
        _consentCookiesDirty = true;
    }

    private static async Task ExportConsentCookiesIfDirtyAsync()
    {
        if (!_consentCookiesDirty || _core is null) return;
        var now = Environment.TickCount64;
        if (now - _lastConsentCookieExportTick < 1000) return;
        _lastConsentCookieExportTick = now;
        _consentCookiesDirty = false;

        try
        {
            var url = !string.IsNullOrWhiteSpace(_core.Source) ? _core.Source : _currentUrl;
            if (string.IsNullOrWhiteSpace(url) || url.StartsWith("about:", StringComparison.OrdinalIgnoreCase)) return;
            var rawCookies = await _core.CookieManager.GetCookiesAsync(url).ConfigureAwait(true);
            var cookies = rawCookies
                .Where(c => !c.IsHttpOnly)
                .Select(c => new SharedConsentCookie(
                    c.Name,
                    c.Value,
                    c.Domain,
                    string.IsNullOrWhiteSpace(c.Path) ? "/" : c.Path,
                    c.Expires <= DateTime.UnixEpoch ? null : new DateTimeOffset(DateTime.SpecifyKind(c.Expires, DateTimeKind.Utc)).ToUnixTimeMilliseconds(),
                    c.IsSecure,
                    c.SameSite.ToString()))
                .Where(IsShareableConsentCookie)
                .Take(64)
                .ToArray();

            if (cookies.Length > 0)
                Send(new { op = "consentCookies", url, cookies });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 consent cookie export failed: " + ex.Message });
        }
    }

    private static bool IsShareableConsentCookie(SharedConsentCookie cookie)
    {
        if (string.IsNullOrWhiteSpace(cookie.Name) || string.IsNullOrWhiteSpace(cookie.Domain)) return false;
        if (string.IsNullOrWhiteSpace(cookie.Value) || cookie.Value.Length > 4096) return false;
        var name = cookie.Name.ToLowerInvariant();

        if (name.Contains("session", StringComparison.Ordinal)
            || name.Contains("auth", StringComparison.Ordinal)
            || name.Contains("token", StringComparison.Ordinal)
            || name.Contains("login", StringComparison.Ordinal)
            || name.Contains("account", StringComparison.Ordinal)
            || name.Contains("passwd", StringComparison.Ordinal)
            || name.Contains("password", StringComparison.Ordinal)
            || name.Contains("csrf", StringComparison.Ordinal)
            || name.Contains("xsrf", StringComparison.Ordinal)
            || name is "sid" or "ssid" or "uid" or "userid" or "user_id")
            return false;

        return name.Contains("consent", StringComparison.Ordinal)
            || name.Contains("cookie", StringComparison.Ordinal)
            || name.Contains("privacy", StringComparison.Ordinal)
            || name.Contains("gdpr", StringComparison.Ordinal)
            || name.Contains("notice", StringComparison.Ordinal)
            || name.Contains("optanon", StringComparison.Ordinal)
            || name.Contains("onetrust", StringComparison.Ordinal)
            || name.Contains("didomi", StringComparison.Ordinal)
            || name.Contains("euconsent", StringComparison.Ordinal)
            || name.Contains("tcf", StringComparison.Ordinal)
            || name.Equals("socs", StringComparison.Ordinal)
            || name.Equals("consent", StringComparison.Ordinal);
    }

    private static CoreWebView2CookieSameSiteKind ParseSameSite(string? value)
        => Enum.TryParse<CoreWebView2CookieSameSiteKind>(value, ignoreCase: true, out var sameSite) ? sameSite : CoreWebView2CookieSameSiteKind.None;

    private static void Navigate(string url)
        => _ = NavigateAsync(url);

    private static async Task NavigateAsync(string url)
    {
        _currentUrl = url;
        if (!_webViewReady || _core is null)
        {
            Send(new { op = "status", text = "WebView2 queued navigation until browser is ready" });
            return;
        }

        await NavigateCoreAsync(url).ConfigureAwait(true);
    }

    private static bool IsDuplicateNavigationRequest(string url)
    {
        var now = Environment.TickCount64;
        var lastUrl = _lastNavigateRequestUrl;
        var lastTick = Interlocked.Read(ref _lastNavigateRequestTick);
        if (!string.IsNullOrWhiteSpace(lastUrl)
            && string.Equals(lastUrl, url, StringComparison.OrdinalIgnoreCase)
            && now - lastTick < 2500)
        {
            return true;
        }

        _lastNavigateRequestUrl = url;
        Interlocked.Exchange(ref _lastNavigateRequestTick, now);
        return false;
    }

    private static async Task NavigateCoreAsync(string url)
    {
        try
        {
            if (IsDuplicateNavigationRequest(url))
            {
                Send(new { op = "status", text = "WebView2 duplicate navigation ignored: " + url });
                return;
            }

            var protectedMedia = IsProtectedMediaUrl(url);
            if (protectedMedia && UseProtectedMediaCompatibilityMode())
            {
                // Optional old compatibility mode for A/B testing only. It is off by default because
                // Prime's AIV CDN is currently returning 403 before playback even reaches DRM.
                await ApplyProtectedMediaModeForUrlAsync(url).ConfigureAwait(true);
                Send(new { op = "status", text = "WebView2 navigating in protected-media compatibility mode: " + url });
            }
            else if (protectedMedia)
            {
                if (!_ravaCastBrowserExtensionsInstalled)
                    await InstallRavaCastBrowserExtensionsAsync().ConfigureAwait(true);
                await ApplyProtectedMediaModeForUrlAsync(url).ConfigureAwait(true);
                Send(new { op = "status", text = "WebView2 navigating with global browser persona and extensions: " + url });
            }
            else
            {
                await EnsureNormalBrowsingToolsInstalledAsync().ConfigureAwait(true);
                await ApplyProtectedMediaModeForUrlAsync(url).ConfigureAwait(true);
                Send(new { op = "status", text = "WebView2 navigating with global browser persona and clean browsing: " + url });
            }

            _core?.Navigate(url);
            // New navigation starts at the beginning until the page reports an actual media position.
            Send(new { op = "mediaState", url, title = string.Empty, isPlaying = true, positionSeconds = 0, durationSeconds = (double?)null });
            ApplyAudioState();
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 navigation failed: " + ex.Message });
        }
    }

    private static void NavigateToBlank()
    {
        try
        {
            _core?.NavigateToString("<html><body style='margin:0;background:#050009'></body></html>");
        }
        catch { }
    }

    private static void QueueMediaSync(double positionSeconds, bool isPlaying, bool force)
    {
        positionSeconds = double.IsFinite(positionSeconds) ? Math.Max(0, positionSeconds) : 0;
        _pendingMediaSyncPosition = positionSeconds;
        _pendingMediaSyncPlaying = isPlaying;
        _pendingMediaSyncUntilTick = Environment.TickCount64 + (force ? 9000 : 3500);
        ApplyPendingMediaSync(force);
    }

    private static void ApplyPendingMediaSync(bool force)
    {
        if (_core is null || _pendingMediaSyncPosition is not { } position) return;
        if (!force && Environment.TickCount64 > _pendingMediaSyncUntilTick)
        {
            _pendingMediaSyncPosition = null;
            return;
        }

        try
        {
            var script = BuildMediaSyncScript(position, _pendingMediaSyncPlaying, force);
            _ = _core.ExecuteScriptAsync(script).ContinueWith(t =>
            {
                if (t.IsFaulted) return;
                try
                {
                    var result = JsonSerializer.Deserialize<bool>(t.Result);
                    if (result)
                        _pendingMediaSyncPosition = null;
                }
                catch { }
            });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 media sync failed: " + ex.Message });
        }
    }

    private static string BuildMediaSyncScript(double positionSeconds, bool isPlaying, bool force)
    {
        var pos = Math.Max(0, positionSeconds).ToString(CultureInfo.InvariantCulture);
        var playing = isPlaying ? "true" : "false";
        var threshold = (force ? 0.20 : 0.75).ToString(CultureInfo.InvariantCulture);
        return "(() => {\n"
            + "  const target = " + pos + ";\n"
            + "  const shouldPlay = " + playing + ";\n"
            + "  const threshold = " + threshold + ";\n"
            + "  let applied = false;\n"
            + "  const media = Array.from(document.querySelectorAll('video,audio')).filter(m => { try { return Number.isFinite(m.duration) || m.readyState > 0; } catch { return false; } });\n"
            + "  for (const m of media) {\n"
            + "    try { if (Number.isFinite(target) && target >= 0 && Math.abs((m.currentTime || 0) - target) > threshold) m.currentTime = target; applied = true; } catch { }\n"
            + "    try { if (shouldPlay && m.paused && m.readyState > 0) m.play().catch(() => {}); else if (!shouldPlay && !m.paused) m.pause(); } catch { }\n"
            + "  }\n"
            + "  const yt = document.querySelector('.html5-video-player');\n"
            + "  if (yt) {\n"
            + "    try { if (typeof yt.seekTo === 'function') { const current = typeof yt.getCurrentTime === 'function' ? Number(yt.getCurrentTime()) : NaN; if (!Number.isFinite(current) || Math.abs(current - target) > threshold) { yt.seekTo(target, true); applied = true; } } } catch { }\n"
            + "    try { if (shouldPlay) yt.playVideo?.(); else yt.pauseVideo?.(); } catch { }\n"
            + "  }\n"
            + "  return applied;\n"
            + "})();";
    }

    private static void PollMediaState()
    {
        if (_core is null || !_webViewReady) return;
        if (Environment.TickCount64 - _lastMediaStateSentTick < 900) return;
        _lastMediaStateSentTick = Environment.TickCount64;

        try
        {
            const string script = @"(() => {
  const safeFinite = v => Number.isFinite(v) ? v : null;
  let best = null;
  for (const m of Array.from(document.querySelectorAll('video,audio'))) {
    try {
      const dur = safeFinite(m.duration);
      const pos = safeFinite(m.currentTime) ?? 0;
      const useful = (dur && dur > 1) || pos > 0.25 || m.readyState > 1;
      if (!useful) continue;
      const score = (dur || 0) + (m.paused ? 0 : 100000) + (m.tagName === 'VIDEO' ? 1000 : 0);
      if (!best || score > best.score) best = { media: m, score };
    } catch { }
  }
  const yt = document.querySelector('.html5-video-player');
  let ytTime = null, ytDuration = null, ytPlaying = null;
  if (yt) {
    try { if (typeof yt.getCurrentTime === 'function') ytTime = safeFinite(yt.getCurrentTime()); } catch { }
    try { if (typeof yt.getDuration === 'function') ytDuration = safeFinite(yt.getDuration()); } catch { }
    try { if (typeof yt.getPlayerState === 'function') ytPlaying = yt.getPlayerState() === 1; } catch { }
  }
  const m = best?.media || null;
  const position = ytTime ?? (m ? (safeFinite(m.currentTime) ?? 0) : 0);
  const duration = ytDuration ?? (m ? safeFinite(m.duration) : null);
  const playing = ytPlaying ?? (m ? !m.paused && !m.ended : false);
  return JSON.stringify({
    url: location.href,
    title: (document.title || '').trim(),
    isPlaying: !!playing,
    positionSeconds: Math.max(0, position || 0),
    durationSeconds: duration && duration > 0 ? duration : null
  });
})()";
            _ = _core.ExecuteScriptAsync(script).ContinueWith(t =>
            {
                if (t.IsFaulted || t.IsCanceled) return;
                try
                {
                    var outer = JsonSerializer.Deserialize<string>(t.Result);
                    if (string.IsNullOrWhiteSpace(outer)) return;
                    using var doc = JsonDocument.Parse(outer);
                    var root = doc.RootElement;
                    var url = root.TryGetProperty("url", out var urlProp) ? urlProp.GetString() ?? _core?.Source ?? _currentUrl ?? string.Empty : _core?.Source ?? _currentUrl ?? string.Empty;
                    var title = root.TryGetProperty("title", out var titleProp) ? titleProp.GetString() ?? string.Empty : string.Empty;
                    var isPlaying = root.TryGetProperty("isPlaying", out var playingProp) && playingProp.ValueKind is JsonValueKind.True or JsonValueKind.False && playingProp.GetBoolean();
                    var position = root.TryGetProperty("positionSeconds", out var posProp) && posProp.ValueKind == JsonValueKind.Number && posProp.TryGetDouble(out var pos) && double.IsFinite(pos) ? Math.Max(0, pos) : 0;
                    double? duration = null;
                    if (root.TryGetProperty("durationSeconds", out var durProp) && durProp.ValueKind == JsonValueKind.Number && durProp.TryGetDouble(out var dur) && double.IsFinite(dur) && dur > 0)
                        duration = dur;
                    Send(new { op = "mediaState", url, title, isPlaying, positionSeconds = position, durationSeconds = duration });
                    if (_pendingMediaSyncPosition is not null)
                        ApplyPendingMediaSync(force: false);
                }
                catch { }
            });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 media state probe failed: " + ex.Message });
        }
    }

    private static void ApplyAudioState()
    {
        try
        {
            ExecuteScript(BuildAudioScript(_muted, _volume));
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 audio command failed: " + ex.Message });
        }
    }

    private static void ExecuteScript(string script)
    {
        try
        {
            if (_core is not null)
                _ = _core.ExecuteScriptAsync(script);
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 script failed: " + ex.Message });
        }
    }

    private static void MaybeRefreshBrowserViewportMetrics()
    {
        var now = Environment.TickCount64;
        if (now - _lastViewportMetricRefreshTick < 750) return;
        RefreshBrowserViewportMetrics();
    }

    private static void RefreshBrowserViewportMetrics()
    {
        try
        {
            if (_core is null) return;
            _lastViewportMetricRefreshTick = Environment.TickCount64;
            const string script = "JSON.stringify({w:Math.max(1,window.innerWidth||document.documentElement.clientWidth||document.body?.clientWidth||0),h:Math.max(1,window.innerHeight||document.documentElement.clientHeight||document.body?.clientHeight||0),dpr:window.devicePixelRatio||1})";
            _ = _core.ExecuteScriptAsync(script).ContinueWith(t =>
            {
                try
                {
                    if (t.IsFaulted || t.IsCanceled || string.IsNullOrWhiteSpace(t.Result)) return;
                    var inner = JsonSerializer.Deserialize<string>(t.Result);
                    if (string.IsNullOrWhiteSpace(inner)) return;
                    using var doc = JsonDocument.Parse(inner);
                    var root = doc.RootElement;
                    var width = root.TryGetProperty("w", out var w) && w.TryGetInt32(out var wi) ? wi : 0;
                    var height = root.TryGetProperty("h", out var h) && h.TryGetInt32(out var hi) ? hi : 0;
                    var dpr = root.TryGetProperty("dpr", out var d) && d.TryGetDouble(out var dpi) ? dpi : 1.0;
                    if (width > 0) _browserViewportWidth = Math.Clamp(width, 1, 16384);
                    if (height > 0) _browserViewportHeight = Math.Clamp(height, 1, 16384);
                    if (dpr > 0) _browserDevicePixelRatio = Math.Clamp(dpr, 0.25, 8.0);
                }
                catch { }
            }, TaskScheduler.Default);
        }
        catch { }
    }

    private static bool TryReadBrowserPointCdp(JsonElement root, out int x, out int y)
    {
        // Chrome DevTools Protocol mouse coordinates are CSS viewport coordinates, not
        // captured-frame pixels and not necessarily WinForms client pixels. Use the page's
        // own window.innerWidth/innerHeight so DPI, WGC scaling, and WebView2 backing-store
        // differences cannot throw the click target off.
        MaybeRefreshBrowserViewportMetrics();
        var cssWidth = Math.Max(1, _browserViewportWidth);
        var cssHeight = Math.Max(1, _browserViewportHeight);

        if (root.TryGetProperty("x", out var nxProp) && root.TryGetProperty("y", out var nyProp))
        {
            var nx = Math.Clamp(nxProp.GetDouble(), 0.0, 1.0);
            var ny = Math.Clamp(nyProp.GetDouble(), 0.0, 1.0);
            x = Math.Clamp((int)Math.Round(nx * (cssWidth - 1)), 0, cssWidth - 1);
            y = Math.Clamp((int)Math.Round(ny * (cssHeight - 1)), 0, cssHeight - 1);
            return true;
        }

        if (root.TryGetProperty("px", out var px) && root.TryGetProperty("py", out var py))
        {
            var sourceWidth = Math.Max(1, ReadInt(root, "sourceWidth"));
            var sourceHeight = Math.Max(1, ReadInt(root, "sourceHeight"));
            if (sourceWidth <= 1) sourceWidth = Math.Max(1, _width);
            if (sourceHeight <= 1) sourceHeight = Math.Max(1, _height);

            x = Math.Clamp((int)Math.Round(px.GetInt32() * cssWidth / (double)sourceWidth), 0, cssWidth - 1);
            y = Math.Clamp((int)Math.Round(py.GetInt32() * cssHeight / (double)sourceHeight), 0, cssHeight - 1);
            return true;
        }

        x = 0;
        y = 0;
        return false;
    }

    private static void ApplyMousePacket(JsonElement root)
    {
        if (!TryReadBrowserPointCdp(root, out var x, out var y)) return;
        var leaving = root.TryGetProperty("leaving", out var leavingProp) && leavingProp.GetBoolean();
        var down = ReadInt(root, "down");
        var up = ReadInt(root, "up");
        var held = ReadInt(root, "held");
        var doubleClick = ReadInt(root, "double");
        var wheelX = ReadInt(root, "wheelX");
        var wheelY = ReadInt(root, "wheelY");
        var modifiers = BuildCdpModifiers(root);

        // Win32 messages into the WebView2 child HWND are unreliable with the Chromium child
        // process and can be swallowed completely. CDP is the supported browser-input path;
        // the important correction is feeding it CSS viewport coordinates, not raw WGC pixels.
        if (leaving)
        {
            var releaseMask = up != 0 ? up : held;
            if (releaseMask != 0)
                DispatchCdpButtonChanges(x, y, root, releaseMask, 0, true, 0);
            DispatchCdpMouseEvent("mouseMoved", x, y, "none", 0, 0, modifiers);
            return;
        }

        DispatchCdpMouseEvent("mouseMoved", x, y, "none", ToCdpButtons(held), 0, modifiers);
        DispatchCdpButtonChanges(x, y, root, down, held | down, false, doubleClick);
        DispatchCdpButtonChanges(x, y, root, up, held & ~up, true, 0);

        if (wheelY != 0)
            DispatchCdpMouseWheel(x, y, wheelX, wheelY, modifiers);
        else if (wheelX != 0)
            DispatchCdpMouseWheel(x, y, wheelX, 0, modifiers);

        // Some protected players gate the Play action behind a real browser-control user gesture.
        // CDP input is good for general browsing, but Prime/Netflix-style players can ignore it for
        // media activation. When protected media mode is active, mirror the same pointer packet to
        // the WebView2 WinForms control as native mouse messages too. This does not bypass DRM; it
        // just makes the embedded browser receive the same click path as a normal Edge window.
        if (_protectedMediaModeActive && (down != 0 || up != 0 || held != 0 || wheelX != 0 || wheelY != 0))
        {
            // Native Win32 mouse messages into the hidden WebView2 HWND can wake Chromium's real
            // cursor handling and cause system-wide pointer blinking. Keep that path opt-in only;
            // CDP remains the normal input path, with the DOM fallback separately gated below.
            if (IsEnvEnabled("RAVACAST_ENABLE_PROTECTED_MEDIA_WIN32_MOUSE_FALLBACK"))
                DispatchWin32MouseInput(root, down, up, held, wheelX, wheelY);
            DispatchProtectedMediaDomPointerFallback(x, y, down, up, held);
        }

        if (down != 0 || up != 0 || wheelX != 0 || wheelY != 0)
        {
            if (down != 0)
                ProbeElementUnderPoint(x, y);
            MarkConsentCookiesDirty();
        }
    }

    private static void DispatchProtectedMediaDomPointerFallback(int x, int y, int down, int up, int held)
    {
        if (_core is null || !_protectedMediaModeActive) return;
        if (!IsEnvEnabled("RAVACAST_ENABLE_PROTECTED_MEDIA_DOM_CLICK_FALLBACK")) return;
        if ((down & 1) == 0 && (up & 1) == 0) return;

        try
        {
            var phase = (down & 1) != 0 ? "down" : "up";
            var script = $$"""
(() => {
    const x = {{x.ToString(CultureInfo.InvariantCulture)}};
    const y = {{y.ToString(CultureInfo.InvariantCulture)}};
    const phase = '{{phase}}';
    const el = document.elementFromPoint(x, y);
    if (!el) return 'none';
    const target = el.closest('button,a,[role="button"],[role="tab"],[tabindex],input,select,textarea') || el;
    try { target.focus && target.focus({ preventScroll: true }); } catch { }
    const base = { bubbles: true, cancelable: true, composed: true, view: window, clientX: x, clientY: y, screenX: x, screenY: y, button: 0, buttons: phase === 'down' ? 1 : 0 };
    try {
        if (window.PointerEvent) target.dispatchEvent(new PointerEvent(phase === 'down' ? 'pointerdown' : 'pointerup', Object.assign({ pointerId: 1, pointerType: 'mouse', isPrimary: true }, base)));
    } catch { }
    try { target.dispatchEvent(new MouseEvent(phase === 'down' ? 'mousedown' : 'mouseup', base)); } catch { }
    if (phase === 'up') {
        try { target.dispatchEvent(new MouseEvent('click', Object.assign({}, base, { detail: 1, buttons: 0 }))); } catch { }
    }
    const id = target.id ? ('#' + target.id) : '';
    const role = target.getAttribute ? (target.getAttribute('role') || '') : '';
    const label = (target.getAttribute && (target.getAttribute('aria-label') || target.getAttribute('title'))) || (target.innerText || target.textContent || '');
    return (target.tagName || 'node').toLowerCase() + id + (role ? '[role=' + role + ']' : '') + ' ' + String(label).replace(/\s+/g, ' ').trim().slice(0, 80);
})()
""";
            _ = _core.ExecuteScriptAsync(script).ContinueWith(t =>
            {
                try
                {
                    if (t.IsFaulted || t.IsCanceled || string.IsNullOrWhiteSpace(t.Result)) return;
                    var target = JsonSerializer.Deserialize<string>(t.Result) ?? "unknown";
                    if (phase == "up") Send(new { op = "status", text = "Protected media DOM click fallback target: " + target });
                }
                catch { }
            }, TaskScheduler.Default);
        }
        catch { }
    }

    private static void DispatchCdpButtonChanges(int x, int y, JsonElement root, int mask, int eventHeldMask, bool mouseUp, int doubleClickMask)
    {
        if ((mask & 1) != 0)
            DispatchCdpMouseEvent(mouseUp ? "mouseReleased" : "mousePressed", x, y, "left", ToCdpButtons(eventHeldMask), (doubleClickMask & 1) != 0 ? 2 : 1, BuildCdpModifiers(root));
        if ((mask & 2) != 0)
            DispatchCdpMouseEvent(mouseUp ? "mouseReleased" : "mousePressed", x, y, "right", ToCdpButtons(eventHeldMask), (doubleClickMask & 2) != 0 ? 2 : 1, BuildCdpModifiers(root));
        if ((mask & 4) != 0)
            DispatchCdpMouseEvent(mouseUp ? "mouseReleased" : "mousePressed", x, y, "middle", ToCdpButtons(eventHeldMask), (doubleClickMask & 4) != 0 ? 2 : 1, BuildCdpModifiers(root));
    }

    private static void ProbeElementUnderPoint(int x, int y)
    {
        try
        {
            if (_core is null) return;
            var script = $"(() => {{ const e = document.elementFromPoint({x.ToString(CultureInfo.InvariantCulture)}, {y.ToString(CultureInfo.InvariantCulture)}); if (!e) return 'none'; const id = e.id ? ('#' + e.id) : ''; const cls = typeof e.className === 'string' && e.className ? ('.' + e.className.split(/\\s+/).slice(0,3).join('.')) : ''; return (e.tagName || 'node').toLowerCase() + id + cls; }})()";
            _ = _core.ExecuteScriptAsync(script).ContinueWith(t =>
            {
                try
                {
                    if (t.IsFaulted || t.IsCanceled) return;
                    var hit = JsonSerializer.Deserialize<string>(t.Result) ?? "unknown";
                    Send(new { op = "status", text = $"WebView2 click target: {hit} @ {x},{y}" });
                }
                catch { }
            }, TaskScheduler.Default);
        }
        catch { }
    }

    private static void ApplyLegacyMouseMoveCommand(JsonElement root)
    {
        if (!TryReadNormalisedBrowserPoint(root, out var x, out var y)) return;
        var packet = new { px = x, py = y, down = 0, up = 0, held = 0, @double = 0, wheelX = 0, wheelY = 0, leaving = false };
        ApplyMousePacket(JsonSerializer.SerializeToElement(packet));
    }

    private static void ApplyLegacyMouseClickCommand(JsonElement root)
    {
        if (!TryReadNormalisedBrowserPoint(root, out var x, out var y)) return;
        var packet = new { px = x, py = y, down = 1, up = 1, held = 0, @double = 0, wheelX = 0, wheelY = 0, leaving = false };
        ApplyMousePacket(JsonSerializer.SerializeToElement(packet));
    }

    private static void ApplyLegacyMouseWheelCommand(JsonElement root)
    {
        if (!TryReadNormalisedBrowserPoint(root, out var x, out var y)) return;
        var wheel = root.TryGetProperty("delta", out var delta) ? Math.Clamp(delta.GetDouble(), -8.0, 8.0) : 0.0;
        var packet = new { px = x, py = y, down = 0, up = 0, held = 0, @double = 0, wheelX = 0, wheelY = -(int)Math.Round(wheel * 100.0), leaving = false };
        ApplyMousePacket(JsonSerializer.SerializeToElement(packet));
    }

    private static void ApplyKeyPacket(JsonElement root)
    {
        var vk = ReadInt(root, "vk");
        if (vk <= 0) return;
        var down = !root.TryGetProperty("down", out var downProp) || downProp.GetBoolean();
        var text = root.TryGetProperty("text", out var textProp) ? textProp.GetString() ?? string.Empty : string.Empty;
        FocusBrowser();
        DispatchCdpKeyEvent(down ? "keyDown" : "keyUp", vk, text, BuildCdpModifiers(root));
        if (down)
            MarkConsentCookiesDirty();
    }

    private static void SendText(string text)
    {
        if (string.IsNullOrEmpty(text)) return;
        FocusBrowser();
        InsertText(text);
        MarkConsentCookiesDirty();
    }

    private static void SendSpecialKey(string key)
    {
        var vk = key.Trim().ToLowerInvariant() switch
        {
            "enter" => VK_RETURN,
            "tab" => VK_TAB,
            "backspace" => VK_BACK,
            "delete" => VK_DELETE,
            "escape" => VK_ESCAPE,
            "left" => VK_LEFT,
            "up" => VK_UP,
            "right" => VK_RIGHT,
            "down" => VK_DOWN,
            "home" => VK_HOME,
            "end" => VK_END,
            _ => 0
        };
        if (vk == 0) return;
        FocusBrowser();
        DispatchCdpKeyEvent("keyDown", vk, string.Empty, 0);
        DispatchCdpKeyEvent("keyUp", vk, string.Empty, 0);
        MarkConsentCookiesDirty();
    }

    private static void FocusBrowser()
    {
        try
        {
            if (_webView is null || _webView.IsDisposed || _core is null) return;

            // The renderer window is deliberately hidden/no-activate beside the game.  Keep focus
            // entirely inside the DevTools/browser input path by default. Repeated JS window.focus()
            // calls can still make Chromium/Windows renegotiate the global cursor even when the
            // WinForms host never activates.
            FireDevToolsProtocol("Emulation.setFocusEmulationEnabled", new { enabled = true });
            if (IsEnvEnabled("RAVACAST_ENABLE_BROWSER_JS_FOCUS"))
                _ = _core.ExecuteScriptAsync("try { window.focus(); document.activeElement?.focus?.({ preventScroll: true }); } catch { }");
        }
        catch { }
    }

    private static void TryRelaxStreamingCookieProfileSettings()
    {
        if (_streamingCookieTrackingPreventionDisabled) return;
        if (TrySetTrackingPreventionLevelNone("Streaming cookie compatibility: WebView2 tracking prevention disabled so cross-site player/CDN cookies can be stored"))
            _streamingCookieTrackingPreventionDisabled = true;
    }

    private static void TryRelaxProtectedMediaProfileSettings()
    {
        TrySetTrackingPreventionLevelNone("Protected media compatibility: tracking prevention relaxed for the RavaCast browser profile");
    }

    private static bool TrySetTrackingPreventionLevelNone(string statusText)
    {
        try
        {
            if (_core is null) return false;
            var profile = _core.Profile;
            var property = profile.GetType().GetProperty("PreferredTrackingPreventionLevel");
            if (property is null || !property.CanWrite)
            {
                Send(new { op = "status", text = "WebView2 tracking prevention setting not exposed by this runtime; leaving it unchanged" });
                return false;
            }

            var previous = property.GetValue(profile)?.ToString() ?? "unknown";
            var value = Enum.Parse(property.PropertyType, "None");
            property.SetValue(profile, value);
            Send(new { op = "status", text = statusText, detail = "previous=" + previous + "; current=None" });
            return true;
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 tracking prevention could not be disabled: " + ex.Message });
            return false;
        }
    }

    private static bool TryReadBrowserPointClient(JsonElement root, out int x, out int y)
    {
        var clientWidth = Math.Max(1, _webView?.ClientSize.Width ?? _width);
        var clientHeight = Math.Max(1, _webView?.ClientSize.Height ?? _height);
        if (root.TryGetProperty("x", out var nxProp) && root.TryGetProperty("y", out var nyProp))
        {
            var nx = Math.Clamp(nxProp.GetDouble(), 0.0, 1.0);
            var ny = Math.Clamp(nyProp.GetDouble(), 0.0, 1.0);
            x = Math.Clamp((int)Math.Round(nx * (clientWidth - 1)), 0, clientWidth - 1);
            y = Math.Clamp((int)Math.Round(ny * (clientHeight - 1)), 0, clientHeight - 1);
            return true;
        }

        if (root.TryGetProperty("px", out var px) && root.TryGetProperty("py", out var py))
        {
            var sourceWidth = Math.Max(1, ReadInt(root, "sourceWidth"));
            var sourceHeight = Math.Max(1, ReadInt(root, "sourceHeight"));
            if (sourceWidth <= 1) sourceWidth = Math.Max(1, _width);
            if (sourceHeight <= 1) sourceHeight = Math.Max(1, _height);
            x = Math.Clamp((int)Math.Round(px.GetInt32() * clientWidth / (double)sourceWidth), 0, clientWidth - 1);
            y = Math.Clamp((int)Math.Round(py.GetInt32() * clientHeight / (double)sourceHeight), 0, clientHeight - 1);
            return true;
        }

        x = y = 0;
        return false;
    }

    private static void DispatchWin32MouseInput(JsonElement root, int down, int up, int held, int wheelX, int wheelY)
    {
        try
        {
            if (_webView is null || _webView.IsDisposed) return;
            var webViewHwnd = _webView.Handle;
            if (webViewHwnd == IntPtr.Zero || !TryReadBrowserPointClient(root, out var x, out var y)) return;
            FocusBrowser();

            var inputHwnd = TryFindChromiumInputChildHwnd(webViewHwnd);
            if (inputHwnd == IntPtr.Zero) inputHwnd = webViewHwnd;

            PostMousePacket(inputHwnd, x, y, down, up, held, wheelY);
            if (inputHwnd != webViewHwnd)
                PostMousePacket(webViewHwnd, x, y, down, up, held, wheelY);
        }
        catch { }
    }

    private static void PostMousePacket(IntPtr hwnd, int x, int y, int down, int up, int held, int wheelY)
    {
        if (hwnd == IntPtr.Zero) return;
        var heldWParam = ToWin32MouseButtons(held);
        var point = MakeLParam(x, y);
        PostMessage(hwnd, WM_MOUSEMOVE, (IntPtr)heldWParam, point);

        if ((down & 1) != 0) PostMessage(hwnd, WM_LBUTTONDOWN, (IntPtr)(heldWParam | MK_LBUTTON), point);
        if ((down & 2) != 0) PostMessage(hwnd, WM_RBUTTONDOWN, (IntPtr)(heldWParam | MK_RBUTTON), point);
        if ((down & 4) != 0) PostMessage(hwnd, WM_MBUTTONDOWN, (IntPtr)(heldWParam | MK_MBUTTON), point);
        if ((up & 1) != 0) PostMessage(hwnd, WM_LBUTTONUP, (IntPtr)(heldWParam & ~MK_LBUTTON), point);
        if ((up & 2) != 0) PostMessage(hwnd, WM_RBUTTONUP, (IntPtr)(heldWParam & ~MK_RBUTTON), point);
        if ((up & 4) != 0) PostMessage(hwnd, WM_MBUTTONUP, (IntPtr)(heldWParam & ~MK_MBUTTON), point);
        if (wheelY != 0) PostMessage(hwnd, WM_MOUSEWHEEL, (IntPtr)((Math.Clamp(wheelY, -1200, 1200) << 16) | heldWParam), point);
    }

    private static IntPtr TryFindChromiumInputChildHwnd(IntPtr webViewHwnd)
    {
        if (webViewHwnd == IntPtr.Zero) return IntPtr.Zero;
        var best = IntPtr.Zero;
        try
        {
            EnumChildWindows(webViewHwnd, (hwnd, _) =>
            {
                var className = GetWindowClassName(hwnd);
                if (className.Contains("Chrome_RenderWidgetHostHWND", StringComparison.OrdinalIgnoreCase)
                    || className.Contains("Chrome_WidgetWin", StringComparison.OrdinalIgnoreCase))
                {
                    best = hwnd;
                    return false;
                }

                return true;
            }, IntPtr.Zero);
        }
        catch { }

        return best;
    }

    private static string GetWindowClassName(IntPtr hwnd)
    {
        try
        {
            var buffer = new System.Text.StringBuilder(256);
            var len = GetClassName(hwnd, buffer, buffer.Capacity);
            return len > 0 ? buffer.ToString() : string.Empty;
        }
        catch
        {
            return string.Empty;
        }
    }

    private static int ToWin32MouseButtons(int mask)
    {
        var buttons = 0;
        if ((mask & 1) != 0) buttons |= MK_LBUTTON;
        if ((mask & 2) != 0) buttons |= MK_RBUTTON;
        if ((mask & 4) != 0) buttons |= MK_MBUTTON;
        return buttons;
    }

    private static IntPtr MakeLParam(int low, int high) => (IntPtr)((high << 16) | (low & 0xFFFF));

    [DllImport("user32.dll")]
    private static extern bool PostMessage(IntPtr hWnd, int msg, IntPtr wParam, IntPtr lParam);

    private static void DispatchCdpMouseEvent(string type, int x, int y, string button, int buttons, int clickCount, int modifiers)
    {
        if (_core is null) return;
        var payload = new
        {
            type,
            x,
            y,
            button,
            buttons,
            clickCount = Math.Max(0, clickCount),
            modifiers
        };
        FireDevToolsProtocol("Input.dispatchMouseEvent", payload);
    }

    private static void DispatchCdpMouseWheel(int x, int y, int deltaX, int deltaY, int modifiers)
    {
        if (_core is null) return;
        var payload = new
        {
            type = "mouseWheel",
            x,
            y,
            button = "none",
            buttons = 0,
            deltaX,
            deltaY,
            modifiers
        };
        FireDevToolsProtocol("Input.dispatchMouseEvent", payload);
    }

    private static void InsertText(string text)
    {
        if (_core is null || string.IsNullOrEmpty(text)) return;
        FireDevToolsProtocol("Input.insertText", new { text });
    }

    private static void DispatchCdpKeyEvent(string type, int vk, string text, int modifiers)
    {
        if (_core is null) return;
        var info = GetKeyInfo(vk);
        var payload = new
        {
            type,
            modifiers,
            windowsVirtualKeyCode = vk,
            nativeVirtualKeyCode = vk,
            key = info.Key,
            code = info.Code,
            text = type == "keyDown" ? NormaliseKeyText(vk, text) : string.Empty,
            unmodifiedText = type == "keyDown" ? NormaliseKeyText(vk, text) : string.Empty
        };
        FireDevToolsProtocol("Input.dispatchKeyEvent", payload);
    }

    private static string NormaliseKeyText(int vk, string text)
    {
        if (vk == VK_RETURN) return "\r";
        if (vk == VK_TAB) return "\t";
        return text ?? string.Empty;
    }

    private static (string Key, string Code) GetKeyInfo(int vk) => vk switch
    {
        VK_BACK => ("Backspace", "Backspace"),
        VK_TAB => ("Tab", "Tab"),
        VK_RETURN => ("Enter", "Enter"),
        VK_ESCAPE => ("Escape", "Escape"),
        VK_DELETE => ("Delete", "Delete"),
        VK_HOME => ("Home", "Home"),
        VK_END => ("End", "End"),
        VK_LEFT => ("ArrowLeft", "ArrowLeft"),
        VK_RIGHT => ("ArrowRight", "ArrowRight"),
        VK_UP => ("ArrowUp", "ArrowUp"),
        VK_DOWN => ("ArrowDown", "ArrowDown"),
        _ => (((char)vk).ToString(), string.Empty)
    };

    private static void FireDevToolsProtocol(string method, object payload)
    {
        try
        {
            if (_core is null) return;
            _ = _core.CallDevToolsProtocolMethodAsync(method, JsonSerializer.Serialize(payload)).ContinueWith(t =>
            {
                if (t.IsFaulted && t.Exception is not null)
                    Send(new { op = "error", message = $"WebView2 input dispatch failed: {t.Exception.GetBaseException().Message}" });
            }, TaskScheduler.Default);
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "WebView2 input dispatch failed: " + ex.Message });
        }
    }

    private static int BuildCdpModifiers(JsonElement root)
    {
        var result = 0;
        if (root.TryGetProperty("alt", out var alt) && alt.GetBoolean()) result |= 1;
        if (root.TryGetProperty("ctrl", out var ctrl) && ctrl.GetBoolean()) result |= 2;
        if (root.TryGetProperty("shift", out var shift) && shift.GetBoolean()) result |= 8;
        return result;
    }

    private static int ToCdpButtons(int heldMask)
    {
        var result = 0;
        if ((heldMask & 1) != 0) result |= 1;
        if ((heldMask & 2) != 0) result |= 2;
        if ((heldMask & 4) != 0) result |= 4;
        return result;
    }

    private static bool TryReadBrowserPointPixels(JsonElement root, out int x, out int y)
    {
        if (root.TryGetProperty("px", out var px) && root.TryGetProperty("py", out var py))
        {
            x = Math.Clamp(px.GetInt32(), 0, Math.Max(0, _width - 1));
            y = Math.Clamp(py.GetInt32(), 0, Math.Max(0, _height - 1));
            return true;
        }
        return TryReadNormalisedBrowserPoint(root, out x, out y);
    }

    private static bool TryReadNormalisedBrowserPoint(JsonElement root, out int x, out int y)
    {
        x = 0;
        y = 0;
        if (_width <= 0 || _height <= 0) return false;
        var normalisedX = root.TryGetProperty("x", out var xProp) ? xProp.GetDouble() : 0.5;
        var normalisedY = root.TryGetProperty("y", out var yProp) ? yProp.GetDouble() : 0.5;
        normalisedX = Math.Clamp(normalisedX, 0.0, 1.0);
        normalisedY = Math.Clamp(normalisedY, 0.0, 1.0);
        x = (int)Math.Round(normalisedX * Math.Max(0, _width - 1));
        y = (int)Math.Round(normalisedY * Math.Max(0, _height - 1));
        return true;
    }

    private static int ReadInt(JsonElement root, string name)
        => root.TryGetProperty(name, out var prop) && prop.TryGetInt32(out var value) ? value : 0;

    private static string BuildAudioScript(bool muted, float volume)
    {
        var v = Math.Clamp(volume, 0.01f, 1f).ToString(CultureInfo.InvariantCulture);
        var p = Math.Clamp((int)MathF.Round(volume * 100f), 1, 100);
        var m = muted ? "true" : "false";
        return "(() => {\n"
            + "  const targetMuted = " + m + ";\n"
            + "  const targetVolume = " + v + ";\n"
            + "  for (const media of Array.from(document.querySelectorAll('video,audio'))) {\n"
            + "    try { media.muted = targetMuted; media.volume = targetVolume; media.dispatchEvent(new Event('volumechange')); } catch { }\n"
            + "  }\n"
            + "  const yt = document.querySelector('.html5-video-player');\n"
            + "  if (yt) { try { if (targetMuted) yt.mute?.(); else yt.unMute?.(); } catch { } try { yt.setVolume?.(" + p + "); } catch { } }\n"
            + "})();";
    }

    private static void ShowInteractiveBrowserWindow()
    {
        if (_form is null || _form.IsDisposed || !_form.IsHandleCreated) return;
        _interactiveBrowserWindowVisible = true;
        try
        {
            ReleaseCursorSterileChildSubclasses();
            if (_form is BrowserHostForm host) host.SetInputSterile(false);
            var area = Screen.PrimaryScreen?.WorkingArea ?? new Rectangle(100, 100, _width, _height);
            var x = Math.Clamp(_form.Location.X, area.Left, Math.Max(area.Left, area.Right - Math.Max(320, _width)));
            var y = Math.Clamp(_form.Location.Y, area.Top, Math.Max(area.Top, area.Bottom - Math.Max(180, _height)));
            SetWindowPos(_form.Handle, HWND_TOPMOST, x, y, _width, _height, SWP_NOACTIVATE | SWP_SHOWWINDOW);
            SetWindowPos(_form.Handle, HWND_NOTOPMOST, x, y, _width, _height, SWP_NOACTIVATE | SWP_SHOWWINDOW);
            Send(new { op = "status", text = "RavaCast browser window shown" });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Could not show RavaCast browser window: " + ex.Message });
        }
    }

    private static void HideInteractiveBrowserWindow()
    {
        _interactiveBrowserWindowVisible = false;
        try
        {
            if (_form is BrowserHostForm host) host.SetInputSterile(true);
            KeepHostBehindGame();
            Send(new { op = "status", text = "RavaCast browser window hidden behind the game" });
        }
        catch (Exception ex)
        {
            Send(new { op = "error", message = "Could not hide RavaCast browser window: " + ex.Message });
        }
    }

    private static void KeepHostBehindGame()
    {
        if (_form is null || _form.IsDisposed || !_form.IsHandleCreated || _interactiveBrowserWindowVisible) return;
        try
        {
            if (_form is BrowserHostForm host) host.SetInputSterile(true);
            RefreshCursorSterileChildSubclasses();

            // Direct Stream only needs a live WebView2 HWND to render/capture from; it does not need
            // that HWND parked under the real game cursor. Keeping a no-activate Chromium window
            // inside the game rectangle still lets Windows/WebView2 participate in WM_SETCURSOR
            // negotiation and causes system-wide pointer flicker for some users. Default to an
            // off-screen, still-visible parking spot. Use the env opt-in only for diagnostics.
            if (!KeepHiddenHostNearGameWindow())
            {
                SetWindowPos(_form.Handle, HWND_BOTTOM, HiddenHostParkingX, HiddenHostParkingY, _width, _height, SWP_NOACTIVATE | SWP_SHOWWINDOW);
                return;
            }

            _gameWindow = _gameWindow != IntPtr.Zero && IsWindow(_gameWindow) ? _gameWindow : FindMainWindowForProcess(_parentPid);
            var insertAfter = _gameWindow != IntPtr.Zero ? _gameWindow : HWND_BOTTOM;
            if (_gameWindow != IntPtr.Zero && TryGetWindowRect(_gameWindow, out var rect))
                SetWindowPos(_form.Handle, insertAfter, rect.Left + 16, rect.Top + 16, _width, _height, SWP_NOACTIVATE | SWP_SHOWWINDOW);
            else
                SetWindowPos(_form.Handle, HWND_BOTTOM, HiddenHostParkingX, HiddenHostParkingY, _width, _height, SWP_NOACTIVATE | SWP_SHOWWINDOW);
        }
        catch { }
    }

    private static bool TryGetGameRect(out Rectangle rect)
    {
        rect = default;
        var hwnd = FindMainWindowForProcess(_parentPid);
        if (hwnd == IntPtr.Zero || !TryGetWindowRect(hwnd, out var nativeRect)) return false;
        rect = Rectangle.FromLTRB(nativeRect.Left, nativeRect.Top, nativeRect.Right, nativeRect.Bottom);
        return true;
    }

    private static IntPtr FindMainWindowForProcess(int pid)
    {
        if (pid <= 0) return IntPtr.Zero;
        try
        {
            using var process = Process.GetProcessById(pid);
            if (process.MainWindowHandle != IntPtr.Zero && IsWindowVisible(process.MainWindowHandle))
                return process.MainWindowHandle;
        }
        catch { }

        var result = IntPtr.Zero;
        EnumWindows((hwnd, _) =>
        {
            GetWindowThreadProcessId(hwnd, out var windowPid);
            if (windowPid != pid || !IsWindowVisible(hwnd) || GetWindow(hwnd, GW_OWNER) != IntPtr.Zero)
                return true;
            result = hwnd;
            return false;
        }, IntPtr.Zero);
        return result;
    }

    private static bool TryGetWindowRect(IntPtr hwnd, out NativeRect rect)
    {
        if (GetWindowRect(hwnd, out rect)) return true;
        rect = default;
        return false;
    }

    private static void StartParentWatch(int parentPid)
    {
        if (parentPid <= 0) return;
        var thread = new Thread(() =>
        {
            try
            {
                using var parent = Process.GetProcessById(parentPid);
                while (!_shutdownRequested)
                {
                    if (parent.HasExited)
                    {
                        _shutdownRequested = true;
                        try { _form?.BeginInvoke(new Action(() => _form.Close())); } catch { }
                        return;
                    }
                    Thread.Sleep(250);
                }
            }
            catch
            {
                _shutdownRequested = true;
            }
        })
        {
            IsBackground = true,
            Name = "RavaCast WebView2 Parent Watch"
        };
        thread.Start();
    }

    private static GraphicsCaptureItem? CreateItemForWindow(IntPtr hwnd)
    {
        var interop = GraphicsCaptureItem.As<IGraphicsCaptureItemInterop>();
        var iid = GraphicsCaptureItemGuid;
        var itemPointer = interop.CreateForWindow(hwnd, ref iid);
        if (itemPointer == IntPtr.Zero) return null;
        try { return GraphicsCaptureItem.FromAbi(itemPointer); }
        finally { Marshal.Release(itemPointer); }
    }

    private static void TrySetOptionalCaptureProperty(GraphicsCaptureSession session, string propertyName, object value)
    {
        try { typeof(GraphicsCaptureSession).GetProperty(propertyName)?.SetValue(session, value); } catch { }
    }

    private static void ShutdownCapture()
    {
        try { _gdiCaptureLoopCts?.Cancel(); } catch { }
        try { _gdiCaptureTimer?.Stop(); } catch { }
        try { _gdiCaptureTimer?.Dispose(); } catch { }
        try { _capturePreviewTimer?.Stop(); } catch { }
        try { _capturePreviewTimer?.Dispose(); } catch { }
        try
        {
            if (_framePool is not null)
                _framePool.FrameArrived -= OnFrameArrived;
        }
        catch { }
        try { _captureSession?.Dispose(); } catch { }
        try { _framePool?.Dispose(); } catch { }
        lock (_gdiCaptureSync)
        {
            try { _gdiCaptureBitmap?.Dispose(); } catch { }
            _gdiCaptureBitmap = null;
        }
        try { _gdiCaptureLoopCts?.Dispose(); } catch { }
        _gdiCaptureLoopCts = null;
        _gdiCaptureLoopTask = null;
        _gdiCaptureTimer = null;
        _capturePreviewTimer = null;
        _usingCapturePreviewFallback = false;
        _capturePreviewInFlight = false;
        Interlocked.Exchange(ref _consecutiveBlankGdiFrames, 0);
        Interlocked.Exchange(ref _lastTrustedNonBlankGdiFrameTick, 0);
        _trustedNonBlankGdiFrames = 0;
        _gdiCaptureHwnd = IntPtr.Zero;
        _sentFirstGdiFrame = false;
        _captureSession = null;
        _framePool = null;
        _captureItem = null;
        _captureReady = false;
    }

    private static void ShutdownD3D()
    {
        try { (_winRtDevice as IDisposable)?.Dispose(); } catch { }
        try { _sharedTexture?.Dispose(); } catch { }
        try { _d3dContext?.Dispose(); } catch { }
        try { _d3dDevice?.Dispose(); } catch { }
        _winRtDevice = null;
        _sharedTexture = null;
        _d3dContext = null;
        _d3dDevice = null;
    }

    private static void Send(object msg)
    {
        try
        {
            var json = JsonSerializer.Serialize(msg);
            LogRenderer("OUT " + json);
            _outgoingMessages.Enqueue(json);
            _sendSignal.Set();
        }
        catch { }
    }

    private static void LogRenderer(string message)
    {
        try
        {
            if (!IsErrorLogMessage(message)) return;
            if (string.IsNullOrWhiteSpace(_rendererLogPath)) return;
            var dir = Path.GetDirectoryName(_rendererLogPath);
            if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir);
            lock (_rendererLogLock)
                File.AppendAllText(_rendererLogPath, DateTimeOffset.Now.ToString("O", CultureInfo.InvariantCulture) + " " + message + Environment.NewLine);
        }
        catch { }
    }

    private static bool IsErrorLogMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message)) return false;
        return message.Contains("\"op\":\"error\"", StringComparison.OrdinalIgnoreCase)
            || message.Contains("error", StringComparison.OrdinalIgnoreCase)
            || message.Contains("failed", StringComparison.OrdinalIgnoreCase)
            || message.Contains("failure", StringComparison.OrdinalIgnoreCase)
            || message.Contains("exception", StringComparison.OrdinalIgnoreCase)
            || message.Contains("fatal", StringComparison.OrdinalIgnoreCase)
            || message.Contains("crash", StringComparison.OrdinalIgnoreCase)
            || message.Contains("unhandled", StringComparison.OrdinalIgnoreCase)
            || message.Contains("could not", StringComparison.OrdinalIgnoreCase)
            || message.Contains("cannot", StringComparison.OrdinalIgnoreCase)
            || message.Contains("unavailable", StringComparison.OrdinalIgnoreCase)
            || message.Contains("missing", StringComparison.OrdinalIgnoreCase)
            || message.Contains("rejected", StringComparison.OrdinalIgnoreCase)
            || message.Contains("unexpected", StringComparison.OrdinalIgnoreCase)
            || message.Contains("broken", StringComparison.OrdinalIgnoreCase);
    }

    private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        => Send(new { op = "error", message = "WebView2 renderer unhandled exception: " + e.ExceptionObject });

    private static void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        Send(new { op = "error", message = "WebView2 renderer task exception: " + e.Exception });
        e.SetObserved();
    }

    private static Dictionary<string, string> ParseArgs(string[] args)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal)) continue;
            var key = args[i][2..];
            if (i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
                dict[key] = args[++i];
            else
                dict[key] = "true";
        }
        return dict;
    }

    private sealed record SharedConsentCookie(string Name, string Value, string Domain, string Path, long? ExpiresUnixMs, bool Secure, string SameSite);


    private static class DirectStreamBridge
    {
        private const string NativeDllName = "RavaCast.Media.Native.dll";
        private const string BridgeHostExeName = "RavaCast.Media.BridgeHost.exe";
        private const string DefaultStunServersJson = "[\"stun:stun.l.google.com:19302\",\"stun:stun1.l.google.com:19302\"]";
        private static readonly object Gate = new();
        private static readonly HashSet<string> Peers = [];
        private static readonly NativeStatusCallback StatusCallback = OnNativeStatus;
        private static readonly NativeSignalCallback SignalCallback = OnNativeSignal;
        private static readonly NativeTextureCallback TextureCallback = OnNativeTexture;
        private static bool Initialised;
        private static bool NativeReady;
        private static string? LastNativeError;
        private static bool PublisherActive;
        private static bool PublisherStartInFlight;
        private static bool ReceiverActive;
        private static IntPtr LastReceiverSharedTextureHandle;
        private static int LastReceiverSharedTextureWidth;
        private static int LastReceiverSharedTextureHeight;
        private static long LastReceiverTextureStatusTick;
        public static IntPtr ReceiverSourceTextureHandle;
        public static D3DTexture2D? ReceiverSourceTexture;
        private static long LastReceiverTextureCopyFailureTick;

        public static bool IsPublisherUsingCurrentSharedTexture
        {
            get
            {
                lock (Gate)
                    return PublisherStartInFlight || PublisherActive || ReceiverActive || Peers.Count > 0;
            }
        }

        public static bool IsReceiverWaitingForFirstSharedTexture
        {
            get
            {
                lock (Gate)
                    return ReceiverActive && LastReceiverSharedTextureHandle == IntPtr.Zero;
            }
        }

        public static bool IsReceiverUsingHostVideoTexture
        {
            get => Volatile.Read(ref ReceiverActive);
        }

        public static void ReleaseReceiverSourceTextureLocked()
        {
            try { ReceiverSourceTexture?.Dispose(); } catch { }
            ReceiverSourceTexture = null;
            ReceiverSourceTextureHandle = IntPtr.Zero;
        }

        public static bool NativeMediaAvailable
        {
            get
            {
                lock (Gate)
                {
                    if (NativeReady) return true;
                    return File.Exists(NativeDllPath) && File.Exists(BridgeHostPath);
                }
            }
        }

        public static string InitialStatusText => NativeMediaAvailable ? "Direct Stream v2 bridge ready" : "Direct Stream files missing";
        public static string? InitialStatusDetail => NativeMediaAvailable
            ? "Direct Stream v2 transport is ready to use libdatachannel with FFmpeg H.264 live video and Opus audio over an RTP media track."
            : "Direct Stream cannot start because the media bridge files are missing from this install.";

        private static string NativeDllPath => Path.Combine(AppContext.BaseDirectory, NativeDllName);
        private static string BridgeHostPath => Path.Combine(AppContext.BaseDirectory, BridgeHostExeName);

        public static void StartPublisher(JsonElement root, IntPtr sharedTextureHandle, int sourceWidth, int sourceHeight)
        {
            var request = new PublisherStartRequest(
                ReadString(root, "castId"),
                sharedTextureHandle,
                sourceWidth,
                sourceHeight,
                ReadInt(root, "width", sourceWidth),
                ReadInt(root, "height", sourceHeight),
                ReadInt(root, "fps", 30),
                ReadInt(root, "videoBitrateKbps", 3500),
                ReadInt(root, "audioBitrateKbps", 128),
                ResolveDirectStreamAudioSourceProcessId());

            if (request.SharedTextureHandle == IntPtr.Zero)
            {
                SendError("Direct Stream cannot start because the source shared D3D texture handle is empty.", false, ReceiverActive);
                return;
            }

            PublisherStartInFlight = true;

            if (!_webViewReady || !_captureReady || !_sentFirstFrame || Interlocked.Read(ref _frameIndex) <= 0)
            {
                Program.Send(new
                {
                    op = "directStreamStatus",
                    text = "Waiting for host preview",
                    detail = "Direct Stream will start as soon as the RavaCast preview produces a live frame. No restart needed.",
                    publisherActive = false,
                    receiverActive = ReceiverActive,
                    nativeMediaAvailable = NativeMediaAvailable
                });
                _ = Task.Run(() => StartPublisherWhenPreviewReadyAsync(request));
                return;
            }

            Program.Send(new
            {
                op = "directStreamStatus",
                text = "Starting Direct Stream v2 publisher",
                detail = $"Cast={request.CastId}; source=0x{request.SharedTextureHandle.ToInt64():X}; source={request.SourceWidth}x{request.SourceHeight}; target={request.TargetWidth}x{request.TargetHeight}@{request.Fps}; video={request.VideoBitrateKbps}kbps audio={request.AudioBitrateKbps}kbps; webView2AudioPid={request.AudioSourceProcessId}; transport=libdatachannel; codecPath=ffmpeg-libav-opus",
                publisherActive = false,
                receiverActive = ReceiverActive,
                nativeMediaAvailable = NativeMediaAvailable
            });

            _ = Task.Run(() => StartPublisherCore(request));
        }

        private static async Task StartPublisherWhenPreviewReadyAsync(PublisherStartRequest request)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(8);
            while (DateTime.UtcNow < deadline)
            {
                if (_shutdownRequested)
                {
                    PublisherStartInFlight = false;
                    return;
                }

                if (_webViewReady && _captureReady && _sentFirstFrame && Interlocked.Read(ref _frameIndex) > 0)
                {
                    StartPublisherCore(request);
                    return;
                }

                try { await Task.Delay(100).ConfigureAwait(false); }
                catch { break; }
            }

            PublisherStartInFlight = false;
            PublisherActive = false;
            SendError("Direct Stream could not start because the host preview did not produce a live frame in time. Open the preview once, then try Direct Stream again.", false, ReceiverActive);
        }

        private static void StartPublisherCore(PublisherStartRequest request)
        {
            request = request with { AudioSourceProcessId = ResolveDirectStreamAudioSourceProcessId() };

            if (!EnsureInitialised())
            {
                PublisherStartInFlight = false;
                PublisherActive = false;
                SendError(BuildMissingOrLoadFailureMessage(), false, ReceiverActive);
                return;
            }

            try
            {
                var rc = RavaCastMedia_StartPublisher(request.SharedTextureHandle, request.SourceWidth, request.SourceHeight, request.TargetWidth, request.TargetHeight, request.Fps, request.VideoBitrateKbps, request.AudioBitrateKbps, request.AudioSourceProcessId, request.CastId, DefaultStunServersJson);
                if (rc != 0)
                {
                    PublisherStartInFlight = false;
                    PublisherActive = false;
                    TryShutdownFailedBridge($"Publisher start failed with bridge return code {rc}.");
                    MarkNativeBridgeNeedsRestart();
                    SendError($"Native Direct Stream publisher failed to start. Bridge return code: {rc}. {LastNativeError}", false, ReceiverActive);
                }
                else
                {
                    // Keep PublisherStartInFlight true until BridgeHost reports publisherActive, so the
                    // shared texture cannot be recreated out from under the bridge during startup.
                    PublisherActive = false;
                    Program.Send(new { op = "directStreamStatus", text = "Direct Stream publisher start queued", detail = "Waiting for BridgeHost to confirm that the shared texture publisher opened successfully.", publisherActive = false, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
                }
            }
            catch (Exception ex)
            {
                PublisherStartInFlight = false;
                PublisherActive = false;
                LastNativeError = ex.Message;
                TryShutdownFailedBridge("Publisher start threw: " + ex.Message);
                MarkNativeBridgeNeedsRestart();
                SendError("Native Direct Stream publisher threw during start: " + ex.Message, false, ReceiverActive);
            }
        }

        private static int ResolveDirectStreamAudioSourceProcessId()
        {
            try
            {
                if (_core is not null)
                {
                    var property = _core.GetType().GetProperty("BrowserProcessId");
                    var value = property?.GetValue(_core);
                    var pid = value switch
                    {
                        int i => i,
                        uint u when u <= int.MaxValue => (int)u,
                        long l when l > 0 && l <= int.MaxValue => (int)l,
                        ulong ul when ul <= int.MaxValue => (int)ul,
                        _ => 0
                    };

                    if (pid > 0)
                        return pid;
                }
            }
            catch { }

            return Environment.ProcessId;
        }

        private sealed record PublisherStartRequest(string CastId, IntPtr SharedTextureHandle, int SourceWidth, int SourceHeight, int TargetWidth, int TargetHeight, int Fps, int VideoBitrateKbps, int AudioBitrateKbps, int AudioSourceProcessId);

        private static void MarkNativeBridgeNeedsRestart()
        {
            lock (Gate)
            {
                Initialised = false;
                NativeReady = false;
            }
        }

        private static void TryShutdownFailedBridge(string reason)
        {
            try
            {
                if (Initialised) RavaCastMedia_Shutdown(reason ?? string.Empty);
            }
            catch (Exception ex)
            {
                LastNativeError = string.IsNullOrWhiteSpace(LastNativeError) ? ex.Message : LastNativeError + " | shutdown: " + ex.Message;
            }
            finally
            {
                PublisherStartInFlight = false;
                PublisherActive = false;
                ReceiverActive = false;
                lock (Gate) Peers.Clear();
            }
        }


        public static void StopPublisher(string reason)
        {
            lock (Gate) Peers.Clear();
            PublisherStartInFlight = false;
            PublisherActive = false;
            Program.Send(new { op = "directStreamStatus", text = "Direct Stream publisher stopping", detail = reason, publisherActive = false, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
            _ = Task.Run(() =>
            {
                try
                {
                    if (Initialised) RavaCastMedia_StopPublisher(reason ?? string.Empty);
                }
                catch (Exception ex)
                {
                    LastNativeError = ex.Message;
                }
                Program.Send(new { op = "directStreamStatus", text = "Direct Stream publisher stopped", detail = reason, publisherActive = false, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
            });
        }

        public static void StartReceiver(JsonElement root)
        {
            var request = new ReceiverStartRequest(
                ReadString(root, "castId"),
                ReadString(root, "hostSessionId"),
                ReadString(root, "viewerSessionId"),
                ReadInt(root, "width", _width),
                ReadInt(root, "height", _height),
                ReadInt(root, "fps", 30),
                ReadInt(root, "videoBitrateKbps", 3500),
                ReadInt(root, "audioBitrateKbps", 128));

            Program.Send(new
            {
                op = "directStreamStatus",
                text = "Starting Direct Stream v2 receiver",
                detail = $"Cast={request.CastId}; host={request.HostSessionId}; viewer={request.ViewerSessionId}; target={request.TargetWidth}x{request.TargetHeight}@{request.Fps}; video={request.VideoBitrateKbps}kbps audio={request.AudioBitrateKbps}kbps",
                publisherActive = PublisherActive,
                receiverActive = true,
                nativeMediaAvailable = NativeMediaAvailable
            });

            ReceiverActive = true;
            _ = Task.Run(() => StartReceiverCore(request));
        }

        private static void StartReceiverCore(ReceiverStartRequest request)
        {
            if (!EnsureInitialised())
            {
                ReceiverActive = false;
                SendError(BuildMissingOrLoadFailureMessage(), PublisherActive, false);
                return;
            }

            try
            {
                var rc = RavaCastMedia_StartReceiver(request.CastId, request.HostSessionId, request.ViewerSessionId, request.TargetWidth, request.TargetHeight, request.Fps, request.VideoBitrateKbps, request.AudioBitrateKbps, DefaultStunServersJson);
                ReceiverActive = rc == 0;
                if (rc != 0)
                {
                    MarkNativeBridgeNeedsRestart();
                    SendError($"Native Direct Stream receiver failed to start. Bridge return code: {rc}. {LastNativeError}", PublisherActive, false);
                }
                else
                {
                    Program.Send(new { op = "directStreamStatus", text = "Direct Stream receiver start queued", detail = "Waiting for the host offer/video track from BridgeHost.", publisherActive = PublisherActive, receiverActive = true, nativeMediaAvailable = NativeMediaAvailable });
                }
            }
            catch (Exception ex)
            {
                ReceiverActive = false;
                LastNativeError = ex.Message;
                MarkNativeBridgeNeedsRestart();
                SendError("Native Direct Stream receiver threw during start: " + ex.Message, PublisherActive, false);
            }
        }

        private sealed record ReceiverStartRequest(string CastId, string HostSessionId, string ViewerSessionId, int TargetWidth, int TargetHeight, int Fps, int VideoBitrateKbps, int AudioBitrateKbps);

        public static void StopReceiver(string reason)
        {
            ReceiverActive = false;
            LastReceiverSharedTextureHandle = IntPtr.Zero;
            LastReceiverSharedTextureWidth = 0;
            LastReceiverSharedTextureHeight = 0;
            LastReceiverTextureStatusTick = 0;
            lock (_d3dLock) ReleaseReceiverSourceTextureLocked();
            _ = Task.Run(() =>
            {
                try
                {
                    if (Initialised) RavaCastMedia_StopReceiver(reason ?? string.Empty);
                }
                catch (Exception ex)
                {
                    LastNativeError = ex.Message;
                }
                Program.Send(new { op = "directStreamStatus", text = "Direct Stream receiver stopped", detail = reason, publisherActive = PublisherActive, receiverActive = false, nativeMediaAvailable = NativeMediaAvailable });
            });
        }

        public static void SetAudio(bool muted, float volume)
        {
            var safeVolume = Math.Clamp(volume, 0f, 1f);
            _ = Task.Run(() =>
            {
                try
                {
                    if (Initialised) RavaCastMedia_SetAudio(muted ? 1 : 0, safeVolume);
                }
                catch (Exception ex)
                {
                    LastNativeError = ex.Message;
                }
            });
        }

        public static void AddPeer(JsonElement root)
        {
            var peerId = ReadString(root, "peerId");
            if (string.IsNullOrWhiteSpace(peerId)) return;
            lock (Gate) Peers.Add(peerId);
            Program.Send(new { op = "directStreamStatus", text = "Viewer connecting", detail = $"Viewer={peerId}; starting media path.", publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable, connectedPeers = Peers.Count });
            _ = Task.Run(() => AddPeerCore(peerId));
        }

        private static void AddPeerCore(string peerId)
        {
            if (!EnsureInitialised())
            {
                Program.Send(new { op = "directStreamStatus", text = "Viewer connecting", detail = $"Viewer={peerId}; native bridge unavailable. {BuildMissingOrLoadFailureMessage()}", publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
                return;
            }

            try
            {
                var rc = RavaCastMedia_AddPeer(peerId);
                if (rc != 0)
                    SendError($"Native Direct Stream viewer add failed for {peerId}. Bridge return code: {rc}. {LastNativeError}", PublisherActive, ReceiverActive);
                else
                    Program.Send(new { op = "directStreamStatus", text = "Viewer registered", detail = $"Viewers={Peers.Count}", publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
            }
            catch (Exception ex)
            {
                LastNativeError = ex.Message;
                SendError($"Native Direct Stream viewer add threw for {peerId}: {ex.Message}", PublisherActive, ReceiverActive);
            }
        }

        public static void RemovePeer(JsonElement root)
        {
            var peerId = ReadString(root, "peerId");
            if (string.IsNullOrWhiteSpace(peerId)) return;
            lock (Gate) Peers.Remove(peerId);

            try
            {
                if (Initialised) RavaCastMedia_RemovePeer(peerId);
            }
            catch (Exception ex)
            {
                LastNativeError = ex.Message;
            }

            Program.Send(new { op = "directStreamStatus", text = Peers.Count == 0 ? "Direct Stream waiting for viewers" : "Viewer left", detail = $"Viewers={Peers.Count}", publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
        }

        public static void HandleSignal(JsonElement root)
        {
            var peerId = ReadString(root, "peerId");
            var signalType = ReadString(root, "signalType");
            var payloadJson = ReadString(root, "payloadJson");
            if (string.IsNullOrWhiteSpace(peerId) || string.IsNullOrWhiteSpace(signalType)) return;
            _ = Task.Run(() => HandleSignalCore(peerId, signalType, payloadJson));
        }

        private static void HandleSignalCore(string peerId, string signalType, string payloadJson)
        {
            if (!EnsureInitialised())
            {
                Program.Send(new { op = "directStreamStatus", text = "Direct Stream signal received", detail = $"{signalType} for viewer {peerId}; native bridge unavailable. {BuildMissingOrLoadFailureMessage()}", publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
                return;
            }

            try
            {
                var rc = RavaCastMedia_HandleSignal(peerId, signalType, payloadJson ?? string.Empty);
                if (rc != 0)
                    SendError($"Native Direct Stream signal handling failed for viewer {peerId}/{signalType}. Bridge return code: {rc}. {LastNativeError}", PublisherActive, ReceiverActive);
            }
            catch (Exception ex)
            {
                LastNativeError = ex.Message;
                SendError($"Native Direct Stream signal handling threw for viewer {peerId}/{signalType}: {ex.Message}", PublisherActive, ReceiverActive);
            }
        }

        public static void StopAll(string reason)
        {
            PublisherStartInFlight = false;
            PublisherActive = false;
            ReceiverActive = false;
            LastReceiverSharedTextureHandle = IntPtr.Zero;
            LastReceiverSharedTextureWidth = 0;
            LastReceiverSharedTextureHeight = 0;
            LastReceiverTextureStatusTick = 0;
            lock (_d3dLock) ReleaseReceiverSourceTextureLocked();
            lock (Gate) Peers.Clear();
            Program.Send(new { op = "directStreamStatus", text = "Direct Stream stopping", detail = reason, publisherActive = false, receiverActive = false, nativeMediaAvailable = NativeMediaAvailable });
            _ = Task.Run(() =>
            {
                try
                {
                    if (Initialised) RavaCastMedia_Shutdown(reason ?? string.Empty);
                }
                catch (Exception ex)
                {
                    LastNativeError = ex.Message;
                }
                Program.Send(new { op = "directStreamStatus", text = "Direct Stream stopped", detail = reason, publisherActive = false, receiverActive = false, nativeMediaAvailable = NativeMediaAvailable });
            });
        }

        private static bool EnsureInitialised()
        {
            lock (Gate)
            {
                if (Initialised) return NativeReady;
                if (!File.Exists(NativeDllPath) || !File.Exists(BridgeHostPath))
                {
                    NativeReady = false;
                    LastNativeError = $"{NativeDllName} or {BridgeHostExeName} is missing from {AppContext.BaseDirectory}";
                    return false;
                }

                try
                {
                    var rc = RavaCastMedia_Initialise(StatusCallback, SignalCallback, TextureCallback);
                    Initialised = true;
                    NativeReady = rc == 0;
                    if (rc != 0)
                    {
                        LastNativeError = $"Initialise returned {rc}.";
                        return false;
                    }

                    Program.Send(new { op = "directStreamStatus", text = "Direct Stream v2 bridge loaded", detail = "libdatachannel transport path is active. FFmpeg H.264 live video and Opus audio over an RTP media track are wired.", publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = true });
                    return true;
                }
                catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException or BadImageFormatException or MarshalDirectiveException)
                {
                    Initialised = false;
                    NativeReady = false;
                    LastNativeError = ex.Message;
                    return false;
                }
                catch (Exception ex)
                {
                    Initialised = false;
                    NativeReady = false;
                    LastNativeError = ex.ToString();
                    return false;
                }
            }
        }

        private static string BuildMissingOrLoadFailureMessage()
        {
            if (!File.Exists(NativeDllPath) || !File.Exists(BridgeHostPath))
                return $"{NativeDllName} / {BridgeHostExeName} were not found beside RavaCast.Renderer.exe. Direct Stream cannot start until the native bridge files are present.";
            return $"{NativeDllName} / {BridgeHostExeName} exist but could not initialise. {LastNativeError}";
        }

        private static void SendError(string message, bool publisherActive, bool receiverActive)
        {
            Program.Send(new { op = "directStreamError", message, publisherActive, receiverActive, nativeMediaAvailable = NativeMediaAvailable });
        }

        private static void OnNativeStatus(string text, string detail, int publisherActive, int receiverActive, int connectedPeers)
        {
            PublisherActive = publisherActive != 0;
            if (PublisherActive || (publisherActive == 0 && text.Contains("publisher stopped", StringComparison.OrdinalIgnoreCase))) PublisherStartInFlight = false;
            ReceiverActive = receiverActive != 0;
            Program.Send(new { op = "directStreamStatus", text = string.IsNullOrWhiteSpace(text) ? "Direct Stream native status" : text, detail = string.IsNullOrWhiteSpace(detail) ? null : detail, publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable, connectedPeers });
        }

        private static void OnNativeSignal(string peerId, string signalType, string payloadJson)
        {
            if (string.IsNullOrWhiteSpace(peerId) || string.IsNullOrWhiteSpace(signalType)) return;
            Program.Send(new { op = "directStreamSignal", peerId, signalType, payloadJson = payloadJson ?? string.Empty });
        }

        private static void OnNativeTexture(IntPtr sharedTextureHandle, int width, int height)
        {
            if (sharedTextureHandle == IntPtr.Zero || width <= 0 || height <= 0) return;

            if (!TryPublishDirectStreamReceiverTexture(sharedTextureHandle, width, height, out var publishedHandle, out var publishedWidth, out var publishedHeight, out var error))
            {
                var nowFailure = Environment.TickCount64;
                if (nowFailure - Interlocked.Read(ref LastReceiverTextureCopyFailureTick) >= 2000)
                {
                    Interlocked.Exchange(ref LastReceiverTextureCopyFailureTick, nowFailure);
                    Program.Send(new { op = "directStreamStatus", text = "Waiting for host video texture", detail = string.IsNullOrWhiteSpace(error) ? "Direct Stream receiver texture is not ready yet." : error, publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
                }
                return;
            }

            var publishTexture = LastReceiverSharedTextureHandle != publishedHandle || LastReceiverSharedTextureWidth != publishedWidth || LastReceiverSharedTextureHeight != publishedHeight;
            if (publishTexture)
            {
                LastReceiverSharedTextureHandle = publishedHandle;
                LastReceiverSharedTextureWidth = publishedWidth;
                LastReceiverSharedTextureHeight = publishedHeight;
                Program.Send(new { op = "sharedTexture", source = "directStreamReceiver", handle = publishedHandle.ToInt64(), width = publishedWidth, height = publishedHeight });
            }

            var now = Environment.TickCount64;
            Program.Send(new { op = "frame", source = "directStreamReceiver", frame = now, width = publishedWidth, height = publishedHeight });

            if (publishTexture || now - Interlocked.Read(ref LastReceiverTextureStatusTick) >= 1000)
            {
                Interlocked.Exchange(ref LastReceiverTextureStatusTick, now);
                Program.Send(new { op = "directStreamStatus", text = "Host video connected", detail = string.Empty, publisherActive = PublisherActive, receiverActive = ReceiverActive, nativeMediaAvailable = NativeMediaAvailable });
            }
        }

        private static string ReadString(JsonElement root, string name) => root.TryGetProperty(name, out var value) ? value.GetString() ?? string.Empty : string.Empty;
        private static int ReadInt(JsonElement root, string name, int fallback = 0) => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number ? value.GetInt32() : fallback;
        private static bool ReadBool(JsonElement root, string name, bool fallback = false) => root.TryGetProperty(name, out var value) && (value.ValueKind == JsonValueKind.True || value.ValueKind == JsonValueKind.False) ? value.GetBoolean() : fallback;
        [UnmanagedFunctionPointer(CallingConvention.StdCall, CharSet = CharSet.Unicode)]
        private delegate void NativeStatusCallback([MarshalAs(UnmanagedType.LPWStr)] string text, [MarshalAs(UnmanagedType.LPWStr)] string detail, int publisherActive, int receiverActive, int connectedPeers);

        [UnmanagedFunctionPointer(CallingConvention.StdCall, CharSet = CharSet.Unicode)]
        private delegate void NativeSignalCallback([MarshalAs(UnmanagedType.LPWStr)] string peerId, [MarshalAs(UnmanagedType.LPWStr)] string signalType, [MarshalAs(UnmanagedType.LPWStr)] string payloadJson);

        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        private delegate void NativeTextureCallback(IntPtr sharedTextureHandle, int width, int height);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_Initialise(NativeStatusCallback statusCallback, NativeSignalCallback signalCallback, NativeTextureCallback textureCallback);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_StartPublisher(IntPtr sharedTextureHandle, int sourceWidth, int sourceHeight, int targetWidth, int targetHeight, int fps, int videoBitrateKbps, int audioBitrateKbps, int audioSourceProcessId, [MarshalAs(UnmanagedType.LPWStr)] string castId, [MarshalAs(UnmanagedType.LPWStr)] string stunServersJson);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_StopPublisher([MarshalAs(UnmanagedType.LPWStr)] string reason);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_StartReceiver([MarshalAs(UnmanagedType.LPWStr)] string castId, [MarshalAs(UnmanagedType.LPWStr)] string hostSessionId, [MarshalAs(UnmanagedType.LPWStr)] string viewerSessionId, int targetWidth, int targetHeight, int fps, int videoBitrateKbps, int audioBitrateKbps, [MarshalAs(UnmanagedType.LPWStr)] string stunServersJson);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_StopReceiver([MarshalAs(UnmanagedType.LPWStr)] string reason);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_AddPeer([MarshalAs(UnmanagedType.LPWStr)] string peerId);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_RemovePeer([MarshalAs(UnmanagedType.LPWStr)] string peerId);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_HandleSignal([MarshalAs(UnmanagedType.LPWStr)] string peerId, [MarshalAs(UnmanagedType.LPWStr)] string signalType, [MarshalAs(UnmanagedType.LPWStr)] string payloadJson);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, ExactSpelling = true)]
        private static extern int RavaCastMedia_SetAudio(int muted, float volume);

        [DllImport(NativeDllName, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.Unicode, ExactSpelling = true)]
        private static extern int RavaCastMedia_Shutdown([MarshalAs(UnmanagedType.LPWStr)] string reason);
    }

    private static void RefreshCursorSterileChildSubclasses()
    {
        if (_interactiveBrowserWindowVisible || _form is null || _form.IsDisposed || !_form.IsHandleCreated)
        {
            ReleaseCursorSterileChildSubclasses();
            return;
        }

        try
        {
            SubclassCursorSterileWindow(_form.Handle);

            if (_webView is not null && !_webView.IsDisposed && _webView.IsHandleCreated)
            {
                SubclassCursorSterileWindow(_webView.Handle);
                EnumChildWindows(_webView.Handle, (hwnd, _) =>
                {
                    SubclassCursorSterileWindow(hwnd);
                    return true;
                }, IntPtr.Zero);
            }

            EnumChildWindows(_form.Handle, (hwnd, _) =>
            {
                SubclassCursorSterileWindow(hwnd);
                return true;
            }, IntPtr.Zero);

            CleanupDeadCursorSterileChildSubclasses();
        }
        catch { }
    }

    private static void SubclassCursorSterileWindow(IntPtr hwnd)
    {
        if (hwnd == IntPtr.Zero || !IsWindow(hwnd)) return;

        // Keep this intentionally broad inside our own renderer process. The visible parent form was
        // already made click-through/no-activate, but WebView2 owns nested Chromium child HWNDs which
        // can still negotiate the global cursor during continuous Direct Stream capture. Subclassing
        // them while the host is hidden stops those children from fighting FFXIV's cursor owner.
        var className = GetWindowClassName(hwnd);
        var formHandle = _form?.Handle ?? IntPtr.Zero;
        var webViewHandle = _webView is { IsDisposed: false, IsHandleCreated: true } ? _webView.Handle : IntPtr.Zero;
        if (hwnd != formHandle && hwnd != webViewHandle && !IsLikelyWebView2CursorWindow(className)) return;

        var proc = Marshal.GetFunctionPointerForDelegate(_cursorSterileChildWndProc);
        lock (_cursorSterileSubclassLock)
        {
            if (_cursorSterileChildWndProcs.ContainsKey(hwnd)) return;

            var current = GetWindowLongPtr(hwnd, GWLP_WNDPROC);
            if (current == IntPtr.Zero || current == proc) return;

            var previous = SetWindowLongPtr(hwnd, GWLP_WNDPROC, proc);
            if (previous != IntPtr.Zero && previous != proc)
                _cursorSterileChildWndProcs[hwnd] = previous;
        }
    }

    private static bool IsLikelyWebView2CursorWindow(string className)
    {
        if (string.IsNullOrWhiteSpace(className)) return false;
        return className.Contains("Chrome", StringComparison.OrdinalIgnoreCase)
            || className.Contains("Chromium", StringComparison.OrdinalIgnoreCase)
            || className.Contains("WebView", StringComparison.OrdinalIgnoreCase)
            || className.Contains("CoreWebView", StringComparison.OrdinalIgnoreCase)
            || className.Contains("Intermediate D3D Window", StringComparison.OrdinalIgnoreCase);
    }

    private static void CleanupDeadCursorSterileChildSubclasses()
    {
        List<IntPtr>? dead = null;
        lock (_cursorSterileSubclassLock)
        {
            foreach (var hwnd in _cursorSterileChildWndProcs.Keys)
            {
                if (IsWindow(hwnd)) continue;
                dead ??= [];
                dead.Add(hwnd);
            }

            if (dead is null) return;
            foreach (var hwnd in dead)
                _cursorSterileChildWndProcs.Remove(hwnd);
        }
    }

    private static void ReleaseCursorSterileChildSubclasses()
    {
        List<KeyValuePair<IntPtr, IntPtr>> restore;
        lock (_cursorSterileSubclassLock)
        {
            if (_cursorSterileChildWndProcs.Count == 0) return;
            restore = _cursorSterileChildWndProcs.ToList();
            _cursorSterileChildWndProcs.Clear();
        }

        foreach (var pair in restore)
        {
            try
            {
                if (pair.Key != IntPtr.Zero && pair.Value != IntPtr.Zero && IsWindow(pair.Key))
                    SetWindowLongPtr(pair.Key, GWLP_WNDPROC, pair.Value);
            }
            catch { }
        }
    }

    private static IntPtr CursorSterileChildWndProc(IntPtr hWnd, int msg, IntPtr wParam, IntPtr lParam)
    {
        if (!_interactiveBrowserWindowVisible && !_shutdownRequested)
        {
            if (msg == WM_SETCURSOR)
                return new IntPtr(1);
            if (msg == WM_MOUSEACTIVATE)
                return new IntPtr(MA_NOACTIVATEANDEAT);
            if (msg == WM_NCHITTEST)
                return new IntPtr(HTTRANSPARENT);
        }

        IntPtr previous;
        lock (_cursorSterileSubclassLock)
        {
            if (!_cursorSterileChildWndProcs.TryGetValue(hWnd, out previous) || previous == IntPtr.Zero)
                return IntPtr.Zero;
        }

        return CallWindowProc(previous, hWnd, msg, wParam, lParam);
    }

    private sealed class CursorMessageFilter : System.Windows.Forms.IMessageFilter
    {
        public bool PreFilterMessage(ref System.Windows.Forms.Message m)
        {
            // Deliberately passive. RavaCast must not force Cursor.Current or swallow
            // WM_SETCURSOR from a hidden/no-activate renderer window while the game owns input.
            return false;
        }
    }

    private sealed class BrowserHostForm : Form
    {
        private bool _inputSterile = true;

        public BrowserHostForm()
        {
            AutoScaleMode = AutoScaleMode.None;
        }

        public void SetInputSterile(bool sterile)
        {
            if (_inputSterile == sterile) return;
            _inputSterile = sterile;
            ApplyInputSterileWindowStyle();
        }

        protected override bool ShowWithoutActivation => true;

        protected override CreateParams CreateParams
        {
            get
            {
                var cp = base.CreateParams;
                cp.ExStyle |= WS_EX_TOOLWINDOW | WS_EX_NOACTIVATE;
                if (_inputSterile) cp.ExStyle |= WS_EX_TRANSPARENT;
                return cp;
            }
        }

        protected override void OnHandleCreated(EventArgs e)
        {
            base.OnHandleCreated(e);
            ApplyInputSterileWindowStyle();
        }

        protected override void WndProc(ref System.Windows.Forms.Message m)
        {
            if (_inputSterile)
            {
                if (m.Msg == WM_NCHITTEST)
                {
                    m.Result = new IntPtr(HTTRANSPARENT);
                    return;
                }

                if (m.Msg == WM_MOUSEACTIVATE)
                {
                    m.Result = new IntPtr(MA_NOACTIVATEANDEAT);
                    return;
                }

                if (m.Msg == WM_SETCURSOR)
                {
                    // While hidden/behind-game, the browser host must not participate in global
                    // cursor selection. Chromium/WebView2 can otherwise fight with FFXIV's cursor
                    // owner and make the real system pointer rapidly blink.
                    m.Result = new IntPtr(1);
                    return;
                }
            }

            base.WndProc(ref m);
        }

        private void ApplyInputSterileWindowStyle()
        {
            if (!IsHandleCreated) return;
            try
            {
                var exStyle = GetWindowLongPtr(Handle, GWL_EXSTYLE).ToInt64();
                if (_inputSterile) exStyle |= WS_EX_TRANSPARENT;
                else exStyle &= ~WS_EX_TRANSPARENT;
                SetWindowLongPtr(Handle, GWL_EXSTYLE, new IntPtr(exStyle));
                SetWindowPos(Handle, IntPtr.Zero, 0, 0, 0, 0, SWP_NOMOVE | SWP_NOSIZE | SWP_NOZORDER | SWP_NOACTIVATE | SWP_FRAMECHANGED);
            }
            catch { }
        }
    }

    [DllImport("d3d11.dll", ExactSpelling = true, PreserveSig = true)]
    private static extern int CreateDirect3D11DeviceFromDXGIDevice(IntPtr dxgiDevice, out IntPtr graphicsDevice);

    [DllImport("user32.dll", EntryPoint = "GetWindowLongPtr", SetLastError = true)]
    private static extern IntPtr GetWindowLongPtr64(IntPtr hWnd, int nIndex);

    [DllImport("user32.dll", EntryPoint = "SetWindowLongPtr", SetLastError = true)]
    private static extern IntPtr SetWindowLongPtr64(IntPtr hWnd, int nIndex, IntPtr dwNewLong);

    [DllImport("user32.dll", EntryPoint = "GetWindowLong", SetLastError = true)]
    private static extern int GetWindowLong32(IntPtr hWnd, int nIndex);

    [DllImport("user32.dll", EntryPoint = "SetWindowLong", SetLastError = true)]
    private static extern int SetWindowLong32(IntPtr hWnd, int nIndex, int dwNewLong);

    private static IntPtr GetWindowLongPtr(IntPtr hWnd, int nIndex)
        => IntPtr.Size == 8 ? GetWindowLongPtr64(hWnd, nIndex) : new IntPtr(GetWindowLong32(hWnd, nIndex));

    private static IntPtr SetWindowLongPtr(IntPtr hWnd, int nIndex, IntPtr dwNewLong)
        => IntPtr.Size == 8 ? SetWindowLongPtr64(hWnd, nIndex, dwNewLong) : new IntPtr(SetWindowLong32(hWnd, nIndex, dwNewLong.ToInt32()));

    private delegate IntPtr WndProcDelegate(IntPtr hWnd, int msg, IntPtr wParam, IntPtr lParam);

    [DllImport("user32.dll", SetLastError = true)]
    private static extern IntPtr CallWindowProc(IntPtr lpPrevWndFunc, IntPtr hWnd, int msg, IntPtr wParam, IntPtr lParam);

    [DllImport("user32.dll")]
    private static extern bool SetWindowPos(IntPtr hWnd, IntPtr hWndInsertAfter, int x, int y, int cx, int cy, uint flags);

    [DllImport("user32.dll")]
    private static extern bool IsWindowVisible(IntPtr hWnd);

    [DllImport("user32.dll")]
    private static extern bool IsWindow(IntPtr hWnd);

    [DllImport("user32.dll")]
    private static extern IntPtr GetWindow(IntPtr hWnd, uint uCmd);

    [DllImport("user32.dll")]
    private static extern bool EnumWindows(EnumWindowsProc lpEnumFunc, IntPtr lParam);

    [DllImport("user32.dll")]
    private static extern bool EnumChildWindows(IntPtr hWndParent, EnumWindowsProc lpEnumFunc, IntPtr lParam);

    [DllImport("user32.dll", CharSet = CharSet.Unicode)]
    private static extern int GetClassName(IntPtr hWnd, System.Text.StringBuilder lpClassName, int nMaxCount);

    [DllImport("user32.dll")]
    private static extern uint GetWindowThreadProcessId(IntPtr hWnd, out int processId);

    [DllImport("user32.dll")]
    private static extern bool GetWindowRect(IntPtr hwnd, out NativeRect lpRect);

    [DllImport("user32.dll")]
    private static extern bool GetClientRect(IntPtr hwnd, out NativeRect lpRect);

    [DllImport("user32.dll")]
    private static extern IntPtr GetDC(IntPtr hWnd);

    [DllImport("user32.dll")]
    private static extern IntPtr GetWindowDC(IntPtr hWnd);

    [DllImport("user32.dll")]
    private static extern int ReleaseDC(IntPtr hWnd, IntPtr hDC);

    [DllImport("user32.dll")]
    private static extern bool PrintWindow(IntPtr hwnd, IntPtr hdcBlt, uint nFlags);

    [DllImport("gdi32.dll")]
    private static extern bool BitBlt(IntPtr hdcDest, int xDest, int yDest, int width, int height, IntPtr hdcSource, int xSrc, int ySrc, uint rop);

    private delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);

    [ComImport]
    [Guid("3628E81B-3CAC-4C60-B7F4-23CE0E0C3356")]
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    private interface IGraphicsCaptureItemInterop
    {
        IntPtr CreateForWindow([In] IntPtr window, [In] ref Guid iid);
        IntPtr CreateForMonitor([In] IntPtr monitor, [In] ref Guid iid);
    }

    [ComImport]
    [Guid("A9B3D012-3DF2-4EE3-B8D1-8695F457D3C1")]
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    private interface IDirect3DDxgiInterfaceAccess
    {
        void GetInterface([In] ref Guid iid, out IntPtr p);
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct NativeRect
    {
        public int Left;
        public int Top;
        public int Right;
        public int Bottom;
    }

    private static readonly IntPtr HWND_TOPMOST = new(-1);
    private static readonly IntPtr HWND_NOTOPMOST = new(-2);
    private static readonly IntPtr HWND_BOTTOM = new(1);
    private const uint GW_OWNER = 4;
    private const uint SWP_NOSIZE = 0x0001;
    private const uint SWP_NOMOVE = 0x0002;
    private const uint SWP_NOZORDER = 0x0004;
    private const uint SWP_NOACTIVATE = 0x0010;
    private const uint SWP_FRAMECHANGED = 0x0020;
    private const uint SWP_SHOWWINDOW = 0x0040;
    private const int GWL_EXSTYLE = -20;
    private const int GWLP_WNDPROC = -4;
    private const int WS_EX_TRANSPARENT = 0x00000020;
    private const int WS_EX_TOOLWINDOW = 0x00000080;
    private const int WS_EX_NOACTIVATE = 0x08000000;
    private const int WM_NCHITTEST = 0x0084;
    private const int WM_MOUSEACTIVATE = 0x0021;
    private const int HTTRANSPARENT = -1;
    private const int MA_NOACTIVATEANDEAT = 4;
    private const uint SRCCOPY = 0x00CC0020;
    private const uint CAPTUREBLT = 0x40000000;
    private const uint PW_RENDERFULLCONTENT = 0x00000002;

    private const int VK_BACK = 0x08;
    private const int VK_TAB = 0x09;
    private const int VK_RETURN = 0x0D;
    private const int VK_ESCAPE = 0x1B;
    private const int VK_END = 0x23;
    private const int VK_HOME = 0x24;
    private const int VK_LEFT = 0x25;
    private const int VK_UP = 0x26;
    private const int VK_RIGHT = 0x27;
    private const int VK_DOWN = 0x28;
    private const int VK_DELETE = 0x2E;
}
