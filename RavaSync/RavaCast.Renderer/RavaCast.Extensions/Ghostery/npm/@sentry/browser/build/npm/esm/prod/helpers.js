import { GLOBAL_OBJ } from "../../../../../core/build/esm/utils/worldwide.js";
import { getLocationHref } from "../../../../../core/build/esm/utils/browser.js";
import { addNonEnumerableProperty, getOriginalFunction, markFunctionWrapped } from "../../../../../core/build/esm/utils/object.js";
import { addExceptionMechanism, addExceptionTypeValue } from "../../../../../core/build/esm/utils/misc.js";
import { withScope } from "../../../../../core/build/esm/currentScopes.js";
import { captureException } from "../../../../../core/build/esm/exports.js";
//#region node_modules/@sentry/browser/build/npm/esm/prod/helpers.js
var WINDOW = GLOBAL_OBJ;
var ignoreOnError = 0;
/**
* @hidden
*/
function shouldIgnoreOnError() {
	return ignoreOnError > 0;
}
/**
* @hidden
*/
function ignoreNextOnError() {
	ignoreOnError++;
	setTimeout(() => {
		ignoreOnError--;
	});
}
/**
* Instruments the given function and sends an event to Sentry every time the
* function throws an exception.
*
* @param fn A function to wrap. It is generally safe to pass an unbound function, because the returned wrapper always
* has a correct `this` context.
* @returns The wrapped function.
* @hidden
*/
function wrap(fn, options = {}) {
	function isFunction(fn) {
		return typeof fn === "function";
	}
	if (!isFunction(fn)) return fn;
	try {
		const wrapper = fn.__sentry_wrapped__;
		if (wrapper) if (typeof wrapper === "function") return wrapper;
		else return fn;
		if (getOriginalFunction(fn)) return fn;
	} catch {
		return fn;
	}
	const sentryWrapped = function(...args) {
		try {
			const wrappedArguments = args.map((arg) => wrap(arg, options));
			return fn.apply(this, wrappedArguments);
		} catch (ex) {
			ignoreNextOnError();
			withScope((scope) => {
				scope.addEventProcessor((event) => {
					if (options.mechanism) {
						addExceptionTypeValue(event, void 0, void 0);
						addExceptionMechanism(event, options.mechanism);
					}
					event.extra = {
						...event.extra,
						arguments: args
					};
					return event;
				});
				captureException(ex);
			});
			throw ex;
		}
	};
	try {
		for (const property in fn) if (Object.prototype.hasOwnProperty.call(fn, property)) sentryWrapped[property] = fn[property];
	} catch {}
	markFunctionWrapped(sentryWrapped, fn);
	addNonEnumerableProperty(fn, "__sentry_wrapped__", sentryWrapped);
	try {
		if (Object.getOwnPropertyDescriptor(sentryWrapped, "name").configurable) Object.defineProperty(sentryWrapped, "name", { get() {
			return fn.name;
		} });
	} catch {}
	return sentryWrapped;
}
/**
* Get HTTP request data from the current page.
*/
function getHttpRequestData() {
	const url = getLocationHref();
	const { referrer } = WINDOW.document || {};
	const { userAgent } = WINDOW.navigator || {};
	return {
		url,
		headers: {
			...referrer && { Referer: referrer },
			...userAgent && { "User-Agent": userAgent }
		}
	};
}
//#endregion
export { WINDOW, getHttpRequestData, shouldIgnoreOnError, wrap };
