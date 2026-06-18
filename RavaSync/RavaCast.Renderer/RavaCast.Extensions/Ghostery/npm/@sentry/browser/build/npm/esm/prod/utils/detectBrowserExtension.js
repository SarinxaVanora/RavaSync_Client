import { consoleSandbox } from "../../../../../../core/build/esm/utils/debug-logger.js";
import { getLocationHref } from "../../../../../../core/build/esm/utils/browser.js";
import { WINDOW } from "../helpers.js";
import { DEBUG_BUILD } from "../debug-build.js";
//#region node_modules/@sentry/browser/build/npm/esm/prod/utils/detectBrowserExtension.js
/**
* Returns true if the SDK is running in an embedded browser extension.
* Stand-alone browser extensions (which do not share the same data as the main browser page) are fine.
*/
function checkAndWarnIfIsEmbeddedBrowserExtension() {
	if (_isEmbeddedBrowserExtension()) {
		if (DEBUG_BUILD) consoleSandbox(() => {
			console.error("[Sentry] You cannot use Sentry.init() in a browser extension, see: https://docs.sentry.io/platforms/javascript/best-practices/browser-extensions/");
		});
		return true;
	}
	return false;
}
function _isEmbeddedBrowserExtension() {
	if (typeof WINDOW.window === "undefined") return false;
	const _window = WINDOW;
	if (_window.nw) return false;
	if (!(_window["chrome"] || _window["browser"])?.runtime?.id) return false;
	const href = getLocationHref();
	return !(WINDOW === WINDOW.top && /^(?:chrome-extension|moz-extension|ms-browser-extension|safari-web-extension):\/\//.test(href));
}
//#endregion
export { checkAndWarnIfIsEmbeddedBrowserExtension };
