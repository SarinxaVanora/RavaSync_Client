import { DEBUG_BUILD } from "../debug-build.js";
import { GLOBAL_OBJ } from "../utils/worldwide.js";
import { CONSOLE_LEVELS, debug, originalConsoleMethods } from "../utils/debug-logger.js";
import { addHandler, maybeInstrument, triggerHandlers } from "./handlers.js";
import { fill } from "../utils/object.js";
import { stringMatchesSomePattern } from "../utils/string.js";
//#region node_modules/@sentry/core/build/esm/instrument/console.js
/**
* Filter out console messages that match the given strings or regular expressions.
* These will neither be passed to the handler, and they will also not be logged to the user, unless they have debug enabled.
* This is a set to avoid duplicate integration setups to add the same filter multiple times.
*/
var _filter = /* @__PURE__ */ new Set([]);
/**
* Add an instrumentation handler for when a console.xxx method is called.
* Returns a function to remove the handler.
*
* Use at your own risk, this might break without changelog notice, only used internally.
* @hidden
*/
function addConsoleInstrumentationHandler(handler) {
	const type = "console";
	const removeHandler = addHandler(type, handler);
	maybeInstrument(type, instrumentConsole);
	return removeHandler;
}
function instrumentConsole() {
	if (!("console" in GLOBAL_OBJ)) return;
	CONSOLE_LEVELS.forEach(function(level) {
		if (!(level in GLOBAL_OBJ.console)) return;
		fill(GLOBAL_OBJ.console, level, function(originalConsoleMethod) {
			originalConsoleMethods[level] = originalConsoleMethod;
			return function(...args) {
				const firstArg = args[0];
				const log = originalConsoleMethods[level];
				const isFiltered = _filter.size && typeof firstArg === "string" && stringMatchesSomePattern(firstArg, _filter);
				if (!isFiltered) triggerHandlers("console", {
					args,
					level
				});
				if (!isFiltered || DEBUG_BUILD && debug.isEnabled()) log?.apply(GLOBAL_OBJ.console, args);
			};
		});
	});
}
//#endregion
export { addConsoleInstrumentationHandler };
