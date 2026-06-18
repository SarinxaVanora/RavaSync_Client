import { AdblockProduct } from "../../utils/adblockers.js";
import { GenericPlatform, SpecificPlatform } from "../platforms.js";
//#region node_modules/@adguard/agtree/dist/compatibility-tables/utils/platform-helpers.js
/**
* @file Provides platform mapping and helper functions.
*/
/**
* Map of specific platforms string names to their corresponding enum values.
*/
var SPECIFIC_PLATFORM_MAP = new Map([
	["adg_os_windows", SpecificPlatform.AdgOsWindows],
	["adg_os_mac", SpecificPlatform.AdgOsMac],
	["adg_os_android", SpecificPlatform.AdgOsAndroid],
	["adg_ext_chrome", SpecificPlatform.AdgExtChrome],
	["adg_ext_opera", SpecificPlatform.AdgExtOpera],
	["adg_ext_edge", SpecificPlatform.AdgExtEdge],
	["adg_ext_firefox", SpecificPlatform.AdgExtFirefox],
	["adg_cb_android", SpecificPlatform.AdgCbAndroid],
	["adg_cb_ios", SpecificPlatform.AdgCbIos],
	["adg_cb_safari", SpecificPlatform.AdgCbSafari],
	["ubo_ext_chrome", SpecificPlatform.UboExtChrome],
	["ubo_ext_opera", SpecificPlatform.UboExtOpera],
	["ubo_ext_edge", SpecificPlatform.UboExtEdge],
	["ubo_ext_firefox", SpecificPlatform.UboExtFirefox],
	["abp_ext_chrome", SpecificPlatform.AbpExtChrome],
	["abp_ext_opera", SpecificPlatform.AbpExtOpera],
	["abp_ext_edge", SpecificPlatform.AbpExtEdge],
	["abp_ext_firefox", SpecificPlatform.AbpExtFirefox]
]);
new Map([...SPECIFIC_PLATFORM_MAP].map(([key, value]) => [value, key]));
new Map([
	["adg_os_any", GenericPlatform.AdgOsAny],
	["adg_safari_any", GenericPlatform.AdgSafariAny],
	["adg_ext_chromium", GenericPlatform.AdgExtChromium],
	["adg_ext_any", GenericPlatform.AdgExtAny],
	["adg_any", GenericPlatform.AdgAny],
	["ubo_ext_chromium", GenericPlatform.UboExtChromium],
	["ubo_ext_any", GenericPlatform.UboExtAny],
	["ubo_any", GenericPlatform.UboAny],
	["abp_ext_chromium", GenericPlatform.AbpExtChromium],
	["abp_ext_any", GenericPlatform.AbpExtAny],
	["abp_any", GenericPlatform.AbpAny],
	["any", GenericPlatform.Any]
]);
AdblockProduct.Adg, AdblockProduct.Ubo, AdblockProduct.Abp;
new Map([
	[SpecificPlatform.AdgOsWindows, "AdGuard App for Windows"],
	[SpecificPlatform.AdgOsMac, "AdGuard App for Mac"],
	[SpecificPlatform.AdgOsAndroid, "AdGuard App for Android"],
	[SpecificPlatform.AdgExtChrome, "AdGuard Browser Extension for Chrome"],
	[SpecificPlatform.AdgExtOpera, "AdGuard Browser Extension for Opera"],
	[SpecificPlatform.AdgExtEdge, "AdGuard Browser Extension for Edge"],
	[SpecificPlatform.AdgExtFirefox, "AdGuard Browser Extension for Firefox"],
	[SpecificPlatform.AdgCbAndroid, "AdGuard Content Blocker for Android"],
	[SpecificPlatform.AdgCbIos, "AdGuard Content Blocker for iOS"],
	[SpecificPlatform.AdgCbSafari, "AdGuard Content Blocker for Safari"],
	[SpecificPlatform.UboExtChrome, "uBlock Origin Browser Extension for Chrome"],
	[SpecificPlatform.UboExtOpera, "uBlock Origin Browser Extension for Opera"],
	[SpecificPlatform.UboExtEdge, "uBlock Origin Browser Extension for Edge"],
	[SpecificPlatform.UboExtFirefox, "uBlock Origin Browser Extension for Firefox"],
	[SpecificPlatform.AbpExtChrome, "AdBlock / Adblock Plus Browser Extension for Chrome"],
	[SpecificPlatform.AbpExtOpera, "AdBlock / Adblock Plus Browser Extension for Opera"],
	[SpecificPlatform.AbpExtEdge, "AdBlock / Adblock Plus Browser Extension for Edge"],
	[SpecificPlatform.AbpExtFirefox, "AdBlock / Adblock Plus Browser Extension for Firefox"]
]);
new Map([
	[GenericPlatform.AdgOsAny, "Any System-level AdGuard App"],
	[GenericPlatform.AdgSafariAny, "Any AdGuard Content Blocker for Safari"],
	[GenericPlatform.AdgExtChromium, "Any AdGuard Browser Extension for Chromium"],
	[GenericPlatform.AdgExtAny, "Any AdGuard Browser Extension"],
	[GenericPlatform.AdgAny, "Any AdGuard product"],
	[GenericPlatform.UboExtChromium, "Any uBlock Origin Browser Extension for Chromium"],
	[GenericPlatform.UboExtAny, "Any uBlock Origin Browser Extension"],
	[GenericPlatform.UboAny, "Any uBlock Origin product"],
	[GenericPlatform.AbpExtChromium, "Any AdBlock / Adblock Plus Browser Extension for Chromium"],
	[GenericPlatform.AbpExtAny, "Any AdBlock / Adblock Plus Browser Extension"],
	[GenericPlatform.AbpAny, "Any AdBlock / Adblock Plus product"],
	[GenericPlatform.Any, "Any product"]
]);
/**
* Check if the platform is a generic platform (or a combination of platforms).
*
* @param platform Platform to check.
*
* @returns True if the platform is a generic platform or combined platforms, false if it's a specific platform.
*/
var isGenericPlatform = (platform) => {
	const num = platform;
	return !!(num & num - 1);
};
//#endregion
export { isGenericPlatform };
