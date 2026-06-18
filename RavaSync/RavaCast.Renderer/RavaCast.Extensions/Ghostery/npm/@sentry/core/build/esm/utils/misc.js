import { GLOBAL_OBJ } from "./worldwide.js";
import { addNonEnumerableProperty } from "./object.js";
import { safeMathRandom, withRandomSafeContext } from "./randomSafeContext.js";
//#region node_modules/@sentry/core/build/esm/utils/misc.js
function getCrypto() {
	const gbl = GLOBAL_OBJ;
	return gbl.crypto || gbl.msCrypto;
}
var emptyUuid;
function getRandomByte() {
	return safeMathRandom() * 16;
}
/**
* UUID4 generator
* @param crypto Object that provides the crypto API.
* @returns string Generated UUID4.
*/
function uuid4(crypto = getCrypto()) {
	try {
		if (crypto?.randomUUID) return withRandomSafeContext(() => crypto.randomUUID()).replace(/-/g, "");
	} catch {}
	if (!emptyUuid) emptyUuid = "10000000100040008000100000000000";
	return emptyUuid.replace(/[018]/g, (c) => (c ^ (getRandomByte() & 15) >> c / 4).toString(16));
}
function getFirstException(event) {
	return event.exception?.values?.[0];
}
/**
* Extracts either message or type+value from an event that can be used for user-facing logs
* @returns event's description
*/
function getEventDescription(event) {
	const { message, event_id: eventId } = event;
	if (message) return message;
	const firstException = getFirstException(event);
	if (firstException) {
		if (firstException.type && firstException.value) return `${firstException.type}: ${firstException.value}`;
		return firstException.type || firstException.value || eventId || "<unknown>";
	}
	return eventId || "<unknown>";
}
/**
* Adds exception values, type and value to an synthetic Exception.
* @param event The event to modify.
* @param value Value of the exception.
* @param type Type of the exception.
* @hidden
*/
function addExceptionTypeValue(event, value, type) {
	const exception = event.exception = event.exception || {};
	const values = exception.values = exception.values || [];
	const firstException = values[0] = values[0] || {};
	if (!firstException.value) firstException.value = value || "";
	if (!firstException.type) firstException.type = type || "Error";
}
/**
* Adds exception mechanism data to a given event. Uses defaults if the second parameter is not passed.
*
* @param event The event to modify.
* @param newMechanism Mechanism data to add to the event.
* @hidden
*/
function addExceptionMechanism(event, newMechanism) {
	const firstException = getFirstException(event);
	if (!firstException) return;
	const defaultMechanism = {
		type: "generic",
		handled: true
	};
	const currentMechanism = firstException.mechanism;
	firstException.mechanism = {
		...defaultMechanism,
		...currentMechanism,
		...newMechanism
	};
	if (newMechanism && "data" in newMechanism) {
		const mergedData = {
			...currentMechanism?.data,
			...newMechanism.data
		};
		firstException.mechanism.data = mergedData;
	}
}
/**
* Checks whether or not we've already captured the given exception (note: not an identical exception - the very object
* in question), and marks it captured if not.
*
* This is useful because it's possible for an error to get captured by more than one mechanism. After we intercept and
* record an error, we rethrow it (assuming we've intercepted it before it's reached the top-level global handlers), so
* that we don't interfere with whatever effects the error might have had were the SDK not there. At that point, because
* the error has been rethrown, it's possible for it to bubble up to some other code we've instrumented. If it's not
* caught after that, it will bubble all the way up to the global handlers (which of course we also instrument). This
* function helps us ensure that even if we encounter the same error more than once, we only record it the first time we
* see it.
*
* Note: It will ignore primitives (always return `false` and not mark them as seen), as properties can't be set on
* them. {@link: Object.objectify} can be used on exceptions to convert any that are primitives into their equivalent
* object wrapper forms so that this check will always work. However, because we need to flag the exact object which
* will get rethrown, and because that rethrowing happens outside of the event processing pipeline, the objectification
* must be done before the exception captured.
*
* @param A thrown exception to check or flag as having been seen
* @returns `true` if the exception has already been captured, `false` if not (with the side effect of marking it seen)
*/
function checkOrSetAlreadyCaught(exception) {
	if (isAlreadyCaptured(exception)) return true;
	try {
		addNonEnumerableProperty(exception, "__sentry_captured__", true);
	} catch {}
	return false;
}
/**
* Checks whether we've already captured the given exception (note: not an identical exception - the very object).
* It is considered already captured if it has the `__sentry_captured__` property set to `true`.
*
* @internal Only considered for internal usage
*/
function isAlreadyCaptured(exception) {
	try {
		return exception.__sentry_captured__;
	} catch {}
}
//#endregion
export { addExceptionMechanism, addExceptionTypeValue, checkOrSetAlreadyCaught, getEventDescription, uuid4 };
