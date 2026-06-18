import { DEBUG_BUILD } from "../debug-build.js";
import { GLOBAL_OBJ } from "./worldwide.js";
import { debug } from "./debug-logger.js";
//#region node_modules/@sentry/core/build/esm/utils/supports.js
var WINDOW = GLOBAL_OBJ;
/**
* Tells whether current environment supports History API
* {@link supportsHistory}.
*
* @returns Answer to the given question.
*/
function supportsHistory() {
	return "history" in WINDOW && !!WINDOW.history;
}
function _isFetchSupported() {
	if (!("fetch" in WINDOW)) return false;
	try {
		new Headers();
		new Request("data:,");
		new Response();
		return true;
	} catch {
		return false;
	}
}
/**
* isNative checks if the given function is a native implementation
*/
function isNativeFunction(func) {
	return func && /^function\s+\w+\(\)\s+\{\s+\[native code\]\s+\}$/.test(func.toString());
}
/**
* Tells whether current environment supports Fetch API natively
* {@link supportsNativeFetch}.
*
* @returns true if `window.fetch` is natively implemented, false otherwise
*/
function supportsNativeFetch() {
	if (typeof EdgeRuntime === "string") return true;
	if (!_isFetchSupported()) return false;
	if (isNativeFunction(WINDOW.fetch)) return true;
	let result = false;
	const doc = WINDOW.document;
	if (doc && typeof doc.createElement === "function") try {
		const sandbox = doc.createElement("iframe");
		sandbox.hidden = true;
		doc.head.appendChild(sandbox);
		if (sandbox.contentWindow?.fetch) result = isNativeFunction(sandbox.contentWindow.fetch);
		doc.head.removeChild(sandbox);
	} catch (err) {
		DEBUG_BUILD && debug.warn("Could not create sandbox iframe for pure fetch check, bailing to window.fetch: ", err);
	}
	return result;
}
//#endregion
export { isNativeFunction, supportsHistory, supportsNativeFetch };
