import { uuid4 } from "./misc.js";
//#region node_modules/@sentry/core/build/esm/utils/propagationContext.js
/**
* Generate a random, valid trace ID.
*/
function generateTraceId() {
	return uuid4();
}
/**
* Generate a random, valid span ID.
*/
function generateSpanId() {
	return uuid4().substring(16);
}
//#endregion
export { generateSpanId, generateTraceId };
