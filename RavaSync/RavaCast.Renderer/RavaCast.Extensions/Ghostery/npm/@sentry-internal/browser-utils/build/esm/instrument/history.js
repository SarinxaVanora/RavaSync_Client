import { addHandler, maybeInstrument, triggerHandlers } from "../../../../../@sentry/core/build/esm/instrument/handlers.js";
import { fill } from "../../../../../@sentry/core/build/esm/utils/object.js";
import { supportsHistory } from "../../../../../@sentry/core/build/esm/utils/supports.js";
import { WINDOW } from "../types.js";
//#region node_modules/@sentry-internal/browser-utils/build/esm/instrument/history.js
var lastHref;
/**
* Add an instrumentation handler for when a fetch request happens.
* The handler function is called once when the request starts and once when it ends,
* which can be identified by checking if it has an `endTimestamp`.
*
* Use at your own risk, this might break without changelog notice, only used internally.
* @hidden
*/
function addHistoryInstrumentationHandler(handler) {
	const type = "history";
	addHandler(type, handler);
	maybeInstrument(type, instrumentHistory);
}
/**
* Exported just for testing
*/
function instrumentHistory() {
	WINDOW.addEventListener("popstate", () => {
		const to = WINDOW.location.href;
		const from = lastHref;
		lastHref = to;
		if (from === to) return;
		triggerHandlers("history", {
			from,
			to
		});
	});
	if (!supportsHistory()) return;
	function historyReplacementFunction(originalHistoryFunction) {
		return function(...args) {
			const url = args.length > 2 ? args[2] : void 0;
			if (url) {
				const from = lastHref;
				const to = getAbsoluteUrl(String(url));
				lastHref = to;
				if (from === to) return originalHistoryFunction.apply(this, args);
				triggerHandlers("history", {
					from,
					to
				});
			}
			return originalHistoryFunction.apply(this, args);
		};
	}
	fill(WINDOW.history, "pushState", historyReplacementFunction);
	fill(WINDOW.history, "replaceState", historyReplacementFunction);
}
function getAbsoluteUrl(urlOrPath) {
	try {
		return new URL(urlOrPath, WINDOW.location.origin).toString();
	} catch {
		return urlOrPath;
	}
}
//#endregion
export { addHistoryInstrumentationHandler };
