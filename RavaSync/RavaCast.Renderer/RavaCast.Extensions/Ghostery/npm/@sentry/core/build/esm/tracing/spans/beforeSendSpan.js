//#region node_modules/@sentry/core/build/esm/tracing/spans/beforeSendSpan.js
/**
* Typesafe check to identify if a `beforeSendSpan` callback expects the streamed span JSON format.
*
* @param callback - The `beforeSendSpan` callback to check.
* @returns `true` if the callback was wrapped with {@link withStreamedSpan}.
*/
function isStreamedBeforeSendSpanCallback(callback) {
	return !!callback && typeof callback === "function" && "_streamed" in callback && !!callback._streamed;
}
//#endregion
export { isStreamedBeforeSendSpanCallback };
