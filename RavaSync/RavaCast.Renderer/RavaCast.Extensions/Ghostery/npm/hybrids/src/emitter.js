import { deferred } from "./utils.js";
//#region node_modules/hybrids/src/emitter.js
var queue = /* @__PURE__ */ new Set();
function add(fn) {
	if (queue.size === 0) deferred.then(execute);
	if (queue.has(fn)) queue.delete(fn);
	queue.add(fn);
}
function clear(fn) {
	queue.delete(fn);
}
function execute() {
	for (const fn of queue) try {
		fn();
	} catch (e) {
		console.error(e);
	}
	queue.clear();
}
//#endregion
export { add, clear };
