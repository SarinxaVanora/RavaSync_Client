import resolveValue from "../resolvers/value.js";
//#region node_modules/hybrids/src/template/helpers/resolve.js
var promiseMap = /* @__PURE__ */ new WeakMap();
function resolve(promise, placeholder, delay = 200) {
	return function fn(host, target) {
		const useLayout = fn.useLayout;
		let timeout;
		if (placeholder) timeout = setTimeout(() => {
			timeout = void 0;
			resolveValue(host, target, placeholder, void 0, useLayout);
		}, delay);
		promiseMap.set(target, promise);
		promise.then((value) => {
			if (timeout) clearTimeout(timeout);
			if (promiseMap.get(target) === promise) {
				resolveValue(host, target, value, placeholder && !timeout ? placeholder : void 0, useLayout);
				promiseMap.set(target, null);
			}
		});
	};
}
//#endregion
export { resolve as default };
