//#region node_modules/@adguard/agtree/dist/compatibility-tables/platforms.js
/**
* @file Provides platform enums.
* The difference between specific and generic platforms is that specific platforms are individual platforms
* (e.g. AdGuard for Windows, AdGuard for Android, etc.),
* while generic platforms are groups of specific platforms
* (e.g. AdGuard for any OS, AdGuard for any Chromium-based extension, etc.).
*/
/**
* List of specific platforms.
*/
var SpecificPlatform = {
	AdgOsWindows: 1,
	AdgOsMac: 2,
	AdgOsAndroid: 4,
	AdgExtChrome: 8,
	AdgExtOpera: 16,
	AdgExtEdge: 32,
	AdgExtFirefox: 64,
	AdgCbAndroid: 128,
	AdgCbIos: 256,
	AdgCbSafari: 512,
	UboExtChrome: 1024,
	UboExtOpera: 2048,
	UboExtEdge: 4096,
	UboExtFirefox: 8192,
	AbpExtChrome: 16384,
	AbpExtOpera: 32768,
	AbpExtEdge: 65536,
	AbpExtFirefox: 1 << 17
};
var AdgOsAny = SpecificPlatform.AdgOsWindows | SpecificPlatform.AdgOsMac | SpecificPlatform.AdgOsAndroid;
var AdgSafariAny = SpecificPlatform.AdgCbSafari | SpecificPlatform.AdgCbIos;
var AdgExtChromium = SpecificPlatform.AdgExtChrome | SpecificPlatform.AdgExtOpera | SpecificPlatform.AdgExtEdge;
var AdgExtAny = AdgExtChromium | SpecificPlatform.AdgExtFirefox;
var AdgAny = AdgExtAny | AdgOsAny | AdgSafariAny | SpecificPlatform.AdgCbAndroid;
var UboExtChromium = SpecificPlatform.UboExtChrome | SpecificPlatform.UboExtOpera | SpecificPlatform.UboExtEdge;
var UboExtAny = UboExtChromium | SpecificPlatform.UboExtFirefox;
var UboAny = UboExtAny;
var AbpExtChromium = SpecificPlatform.AbpExtChrome | SpecificPlatform.AbpExtOpera | SpecificPlatform.AbpExtEdge;
var AbpExtAny = AbpExtChromium | SpecificPlatform.AbpExtFirefox;
var AbpAny = AbpExtAny;
/**
* List of generic platforms (combinations of specific platforms).
*/
var GenericPlatform = {
	AdgOsAny,
	AdgSafariAny,
	AdgExtChromium,
	AdgExtAny,
	AdgAny,
	UboExtChromium,
	UboExtAny,
	UboAny,
	AbpExtChromium,
	AbpExtAny,
	AbpAny,
	Any: AdgAny | UboAny | AbpAny
};
//#endregion
export { GenericPlatform, SpecificPlatform };
