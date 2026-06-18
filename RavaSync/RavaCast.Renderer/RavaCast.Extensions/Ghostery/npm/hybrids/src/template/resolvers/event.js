//#region node_modules/hybrids/src/template/resolvers/event.js
var targets = /* @__PURE__ */ new WeakMap();
function resolveEventListener(eventType) {
	return (host, target, value, lastValue) => {
		if (lastValue) {
			const eventMap = targets.get(target);
			target.removeEventListener(eventType, eventMap.get(lastValue), lastValue.options !== void 0 ? lastValue.options : false);
		}
		if (value) {
			if (typeof value !== "function") throw Error(`Event listener must be a function: ${typeof value}`);
			let eventMap = targets.get(target);
			if (!eventMap) {
				eventMap = /* @__PURE__ */ new WeakMap();
				targets.set(target, eventMap);
			}
			const callback = value.bind(null, host);
			eventMap.set(value, callback);
			target.addEventListener(eventType, callback, value.options !== void 0 ? value.options : false);
		}
	};
}
//#endregion
export { resolveEventListener as default };
