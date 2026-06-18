//#region node_modules/@sentry/core/build/esm/tracing/spans/hasSpanStreamingEnabled.js
/**
* Determines if span streaming is enabled for the given client
*/
function hasSpanStreamingEnabled(client) {
	return client.getOptions().traceLifecycle === "stream";
}
//#endregion
export { hasSpanStreamingEnabled };
