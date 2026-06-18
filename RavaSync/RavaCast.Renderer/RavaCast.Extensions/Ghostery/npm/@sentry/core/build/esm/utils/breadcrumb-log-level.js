//#region node_modules/@sentry/core/build/esm/utils/breadcrumb-log-level.js
/**
* Determine a breadcrumb's log level (only `warning` or `error`) based on an HTTP status code.
*/
function getBreadcrumbLogLevelFromHttpStatusCode(statusCode) {
	if (statusCode === void 0) return;
	else if (statusCode >= 400 && statusCode < 500) return "warning";
	else if (statusCode >= 500) return "error";
	else return;
}
//#endregion
export { getBreadcrumbLogLevelFromHttpStatusCode };
