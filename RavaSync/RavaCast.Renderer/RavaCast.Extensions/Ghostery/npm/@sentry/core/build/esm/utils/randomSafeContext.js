import { GLOBAL_OBJ } from "./worldwide.js";
//#region node_modules/@sentry/core/build/esm/utils/randomSafeContext.js
var RESOLVED_RUNNER;
/**
* Simple wrapper that allows SDKs to *secretly* set context wrapper to generate safe random IDs in cache components contexts
*/
function withRandomSafeContext(cb) {
	if (RESOLVED_RUNNER !== void 0) return RESOLVED_RUNNER ? RESOLVED_RUNNER(cb) : cb();
	const sym = Symbol.for("__SENTRY_SAFE_RANDOM_ID_WRAPPER__");
	const globalWithSymbol = GLOBAL_OBJ;
	if (sym in globalWithSymbol && typeof globalWithSymbol[sym] === "function") {
		RESOLVED_RUNNER = globalWithSymbol[sym];
		return RESOLVED_RUNNER(cb);
	}
	RESOLVED_RUNNER = null;
	return cb();
}
/**
* Identical to Math.random() but wrapped in withRandomSafeContext
* to ensure safe random number generation in certain contexts (e.g., Next.js Cache Components).
*/
function safeMathRandom() {
	return withRandomSafeContext(() => Math.random());
}
/**
* Identical to Date.now() but wrapped in withRandomSafeContext
* to ensure safe time value generation in certain contexts (e.g., Next.js Cache Components).
*/
function safeDateNow() {
	return withRandomSafeContext(() => Date.now());
}
//#endregion
export { safeDateNow, safeMathRandom, withRandomSafeContext };
