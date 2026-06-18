//#region node_modules/@sentry/core/build/esm/tracing/spans/captureSpan.js
/**
* Safely set attributes on a span JSON.
* If an attribute already exists, it will not be overwritten.
*/
function safeSetSpanJSONAttributes(spanJSON, newAttributes) {
	const originalAttributes = spanJSON.attributes ?? (spanJSON.attributes = {});
	Object.entries(newAttributes).forEach(([key, value]) => {
		if (value != null && !(key in originalAttributes)) originalAttributes[key] = value;
	});
}
//#endregion
export { safeSetSpanJSONAttributes };
