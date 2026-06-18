//#region node_modules/@sentry/core/build/esm/utils/env.js
/**
* Figures out if we're building a browser bundle.
*
* @returns true if this is a browser bundle build.
*/
function isBrowserBundle() {
	return typeof __SENTRY_BROWSER_BUNDLE__ !== "undefined" && !!__SENTRY_BROWSER_BUNDLE__;
}
/**
* Get source of SDK.
*/
function getSDKSource() {
	return "npm";
}
//#endregion
export { getSDKSource, isBrowserBundle };
