//#region node_modules/@sentry/core/build/esm/utils/weakRef.js
/**
* Resolves a potentially weak reference, returning the underlying object
* or undefined if the reference has been garbage collected.
*
* @param ref - A MaybeWeakRef or undefined
* @returns The referenced object, or undefined if GC'd or ref was undefined
*/
function derefWeakRef(ref) {
	if (!ref) return;
	if (typeof ref === "object" && "deref" in ref && typeof ref.deref === "function") try {
		return ref.deref();
	} catch {
		return;
	}
	return ref;
}
//#endregion
export { derefWeakRef };
