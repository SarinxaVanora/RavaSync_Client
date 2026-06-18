//#region node_modules/@sentry/core/build/esm/utils/normalizationHints.js
/**
* Internal symbols for normalization behavior. JSON and other structured user payloads cannot
* carry these keys, so they cannot spoof SDK-only normalization hints.
* We use Symbol.for to ensure that the symbols are the same across different modules/files.
*/
var SENTRY_SKIP_NORMALIZATION = Symbol.for("sentry.skipNormalization");
var SENTRY_OVERRIDE_NORMALIZATION_DEPTH = Symbol.for("sentry.overrideNormalizationDepth");
/** @internal */
function hasSkipNormalizationHint(value) {
	return Boolean(value[SENTRY_SKIP_NORMALIZATION]);
}
/** @internal */
function getNormalizationDepthOverrideHint(value) {
	const v = value[SENTRY_OVERRIDE_NORMALIZATION_DEPTH];
	return typeof v === "number" ? v : void 0;
}
//#endregion
export { getNormalizationDepthOverrideHint, hasSkipNormalizationHint };
