import { GLOBAL_OBJ } from "../utils/worldwide.js";
import { addHandler, maybeInstrument, triggerHandlers } from "./handlers.js";
//#region node_modules/@sentry/core/build/esm/instrument/globalUnhandledRejection.js
var _oldOnUnhandledRejectionHandler = null;
/**
* Add an instrumentation handler for when an unhandled promise rejection is captured.
*
* Use at your own risk, this might break without changelog notice, only used internally.
* @hidden
*/
function addGlobalUnhandledRejectionInstrumentationHandler(handler) {
	const type = "unhandledrejection";
	addHandler(type, handler);
	maybeInstrument(type, instrumentUnhandledRejection);
}
function instrumentUnhandledRejection() {
	_oldOnUnhandledRejectionHandler = GLOBAL_OBJ.onunhandledrejection;
	GLOBAL_OBJ.onunhandledrejection = function(e) {
		triggerHandlers("unhandledrejection", e);
		if (_oldOnUnhandledRejectionHandler) return _oldOnUnhandledRejectionHandler.apply(this, arguments);
		return true;
	};
	GLOBAL_OBJ.onunhandledrejection.__SENTRY_INSTRUMENTED__ = true;
}
//#endregion
export { addGlobalUnhandledRejectionInstrumentationHandler };
