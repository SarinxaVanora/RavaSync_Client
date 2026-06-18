import { GLOBAL_OBJ } from "./worldwide.js";
import { safeDateNow, withRandomSafeContext } from "./randomSafeContext.js";
//#region node_modules/@sentry/core/build/esm/utils/time.js
var ONE_SECOND_IN_MS = 1e3;
/**
* A partial definition of the [Performance Web API]{@link https://developer.mozilla.org/en-US/docs/Web/API/Performance}
* for accessing a high-resolution monotonic clock.
*/
/**
* Returns a timestamp in seconds since the UNIX epoch using the Date API.
*/
function dateTimestampInSeconds() {
	return safeDateNow() / ONE_SECOND_IN_MS;
}
/**
* Returns a wrapper around the native Performance API browser implementation, or undefined for browsers that do not
* support the API.
*
* Wrapping the native API works around differences in behavior from different browsers.
*/
function createUnixTimestampInSecondsFunc() {
	const { performance } = GLOBAL_OBJ;
	if (!performance?.now || !performance.timeOrigin) return dateTimestampInSeconds;
	const timeOrigin = performance.timeOrigin;
	return () => {
		return (timeOrigin + withRandomSafeContext(() => performance.now())) / ONE_SECOND_IN_MS;
	};
}
var _cachedTimestampInSeconds;
/**
* Returns a timestamp in seconds since the UNIX epoch using either the Performance or Date APIs, depending on the
* availability of the Performance API.
*
* BUG: Note that because of how browsers implement the Performance API, the clock might stop when the computer is
* asleep. This creates a skew between `dateTimestampInSeconds` and `timestampInSeconds`. The
* skew can grow to arbitrary amounts like days, weeks or months.
* See https://github.com/getsentry/sentry-javascript/issues/2590.
*/
function timestampInSeconds() {
	return (_cachedTimestampInSeconds ?? (_cachedTimestampInSeconds = createUnixTimestampInSecondsFunc()))();
}
//#endregion
export { dateTimestampInSeconds, timestampInSeconds };
