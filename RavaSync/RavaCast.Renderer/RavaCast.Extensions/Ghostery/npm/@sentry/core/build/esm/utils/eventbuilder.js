import { isError } from "./is.js";
//#region node_modules/@sentry/core/build/esm/utils/eventbuilder.js
function hasSentryFetchUrlHost(error) {
	return isError(error) && "__sentry_fetch_url_host__" in error && typeof error.__sentry_fetch_url_host__ === "string";
}
/**
* Enhances the error message with the hostname for better Sentry error reporting.
* This allows third-party packages to still match on the original error message,
* while Sentry gets the enhanced version with context.
*
* Only used internally
* @hidden
*/
function _enhanceErrorWithSentryInfo(error) {
	if (hasSentryFetchUrlHost(error)) return `${error.message} (${error.__sentry_fetch_url_host__})`;
	return error.message;
}
//#endregion
export { _enhanceErrorWithSentryInfo };
