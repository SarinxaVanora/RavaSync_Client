import { GLOBAL_OBJ } from "../utils/worldwide.js";
import { addHandler, maybeInstrument, triggerHandlers } from "./handlers.js";
//#region node_modules/@sentry/core/build/esm/instrument/globalError.js
var _oldOnErrorHandler = null;
/**
* Add an instrumentation handler for when an error is captured by the global error handler.
*
* Use at your own risk, this might break without changelog notice, only used internally.
* @hidden
*/
function addGlobalErrorInstrumentationHandler(handler) {
	const type = "error";
	addHandler(type, handler);
	maybeInstrument(type, instrumentError);
}
function instrumentError() {
	_oldOnErrorHandler = GLOBAL_OBJ.onerror;
	GLOBAL_OBJ.onerror = function(msg, url, line, column, error) {
		triggerHandlers("error", {
			column,
			error,
			line,
			msg,
			url
		});
		if (_oldOnErrorHandler) return _oldOnErrorHandler.apply(this, arguments);
		return false;
	};
	GLOBAL_OBJ.onerror.__SENTRY_INSTRUMENTED__ = true;
}
//#endregion
export { addGlobalErrorInstrumentationHandler };
