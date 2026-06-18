import { isNotParsing } from "./parse-from-string.js";
//#region node_modules/linkedom/esm/shared/cache.js
var childNodesWM = /* @__PURE__ */ new WeakMap();
var childrenWM = /* @__PURE__ */ new WeakMap();
var querySelectorWM = /* @__PURE__ */ new WeakMap();
var querySelectorAllWM = /* @__PURE__ */ new WeakMap();
var get = (wm, self, method) => {
	if (wm.has(self)) return wm.get(self);
	const value = method.call(self);
	wm.set(self, value);
	return value;
};
var reset = (parentNode) => {
	if (isNotParsing()) while (parentNode) {
		childNodesWM.delete(parentNode);
		childrenWM.delete(parentNode);
		querySelectorWM.delete(parentNode);
		querySelectorAllWM.delete(parentNode);
		parentNode = parentNode.parentNode;
	}
};
//#endregion
export { childNodesWM, childrenWM, get, querySelectorAllWM, querySelectorWM, reset };
