import { debug } from "../../../../@sentry/core/build/esm/utils/debug-logger.js";
import { isNativeFunction } from "../../../../@sentry/core/build/esm/utils/supports.js";
import { DEBUG_BUILD } from "./debug-build.js";
import { WINDOW } from "./types.js";
//#region node_modules/@sentry-internal/browser-utils/build/esm/getNativeImplementation.js
/**
* We generally want to use window.fetch / window.setTimeout.
* However, in some cases this may be wrapped (e.g. by Zone.js for Angular),
* so we try to get an unpatched version of this from a sandboxed iframe.
*/
var cachedImplementations = {};
/**
* Get the native implementation of a browser function.
*
* This can be used to ensure we get an unwrapped version of a function, in cases where a wrapped function can lead to problems.
*
* The following methods can be retrieved:
* - `setTimeout`: This can be wrapped by e.g. Angular, causing change detection to be triggered.
* - `fetch`: This can be wrapped by e.g. ad-blockers, causing an infinite loop when a request is blocked.
*/
function getNativeImplementation(name) {
	const cached = cachedImplementations[name];
	if (cached) return cached;
	let impl = WINDOW[name];
	if (isNativeFunction(impl)) return cachedImplementations[name] = impl.bind(WINDOW);
	const document = WINDOW.document;
	if (document && typeof document.createElement === "function") try {
		const sandbox = document.createElement("iframe");
		sandbox.hidden = true;
		document.head.appendChild(sandbox);
		const contentWindow = sandbox.contentWindow;
		if (contentWindow?.[name]) impl = contentWindow[name];
		document.head.removeChild(sandbox);
	} catch (e) {
		DEBUG_BUILD && debug.warn(`Could not create sandbox iframe for ${name} check, bailing to window.${name}: `, e);
	}
	if (!impl) return impl;
	return cachedImplementations[name] = impl.bind(WINDOW);
}
/** Clear a cached implementation. */
function clearCachedImplementation(name) {
	cachedImplementations[name] = void 0;
}
//#endregion
export { clearCachedImplementation, getNativeImplementation };
