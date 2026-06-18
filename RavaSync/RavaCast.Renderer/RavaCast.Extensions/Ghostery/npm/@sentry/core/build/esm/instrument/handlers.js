import { DEBUG_BUILD } from "../debug-build.js";
import { debug } from "../utils/debug-logger.js";
import { getFunctionName } from "../utils/stacktrace.js";
//#region node_modules/@sentry/core/build/esm/instrument/handlers.js
var handlers = {};
var instrumented = {};
/** Add a handler function. Returns a function to remove the handler. */
function addHandler(type, handler) {
	handlers[type] = handlers[type] || [];
	handlers[type].push(handler);
	return () => {
		const typeHandlers = handlers[type];
		if (typeHandlers) {
			const index = typeHandlers.indexOf(handler);
			if (index !== -1) typeHandlers.splice(index, 1);
		}
	};
}
/** Maybe run an instrumentation function, unless it was already called. */
function maybeInstrument(type, instrumentFn) {
	if (!instrumented[type]) {
		instrumented[type] = true;
		try {
			instrumentFn();
		} catch (e) {
			DEBUG_BUILD && debug.error(`Error while instrumenting ${type}`, e);
		}
	}
}
/** Trigger handlers for a given instrumentation type. */
function triggerHandlers(type, data) {
	const typeHandlers = type && handlers[type];
	if (!typeHandlers) return;
	for (const handler of typeHandlers) try {
		handler(data);
	} catch (e) {
		DEBUG_BUILD && debug.error(`Error while triggering instrumentation handler.\nType: ${type}\nName: ${getFunctionName(handler)}\nError:`, e);
	}
}
//#endregion
export { addHandler, maybeInstrument, triggerHandlers };
