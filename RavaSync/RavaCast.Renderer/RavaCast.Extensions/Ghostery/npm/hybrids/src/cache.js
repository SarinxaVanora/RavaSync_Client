import { add } from "./emitter.js";
//#region node_modules/hybrids/src/cache.js
var entries = /* @__PURE__ */ new WeakMap();
var stack = /* @__PURE__ */ new Set();
function dispatch(entry, resolved = false) {
	const contexts = [];
	let index = 0;
	entry.resolved = resolved;
	while (entry) {
		if (entry.contexts) {
			for (const context of entry.contexts) if (!stack.has(context) && !contexts.includes(context)) {
				context.resolved = false;
				contexts.push(context);
			}
		}
		if (entry.observe) add(entry.observe);
		entry = contexts[index++];
	}
}
function getEntry(target, key) {
	let map = entries.get(target);
	if (!map) {
		map = /* @__PURE__ */ new Map();
		entries.set(target, map);
	}
	let entry = map.get(key);
	if (!entry) {
		entry = {
			key,
			target,
			value: void 0,
			assertValue: void 0,
			lastValue: void 0,
			resolved: false,
			contexts: void 0,
			deps: void 0,
			observe: void 0
		};
		map.set(key, entry);
	}
	return entry;
}
function getEntries(target) {
	const targetMap = entries.get(target);
	if (targetMap) return [...targetMap.values()];
	return [];
}
var context = null;
function getCurrentValue() {
	return context?.value;
}
function get(target, key, fn) {
	const entry = getEntry(target, key);
	if (context) {
		if (!entry.contexts) entry.contexts = /* @__PURE__ */ new Set();
		if (!context.deps) context.deps = /* @__PURE__ */ new Set();
		entry.contexts.add(context);
		context.deps.add(entry);
	}
	if (entry.resolved) return entry.value;
	if (entry.deps) {
		for (const depEntry of entry.deps) depEntry.contexts.delete(entry);
		entry.deps.clear();
	}
	const lastContext = context;
	try {
		if (stack.has(entry)) throw Error(`Circular get invocation is forbidden: '${key}'`);
		context = entry;
		stack.add(entry);
		entry.value = fn(target, entry.assertValue);
		entry.resolved = true;
		context = lastContext;
		stack.delete(entry);
	} catch (e) {
		context = lastContext;
		stack.delete(entry);
		if (context) {
			context.deps.delete(entry);
			entry.contexts.delete(context);
		}
		throw e;
	}
	return entry.value;
}
function assert(target, key, value, force) {
	if (context && context.target === target && !force) throw Error(`Try to update the '${key}' property while getting the '${context.key}' property`);
	const entry = getEntry(target, key);
	entry.value = void 0;
	entry.assertValue = value;
	dispatch(entry);
}
function sync(target, key, fn, value) {
	const entry = getEntry(target, key);
	const nextValue = fn(target, value, entry.value);
	if (nextValue !== entry.value) {
		entry.value = nextValue;
		entry.assertValue = void 0;
		dispatch(entry, true);
		entry.resolved = true;
	}
}
function observe(target, key, fn, callback) {
	const entry = getEntry(target, key);
	entry.observe = () => {
		const value = get(target, key, fn);
		if (value !== entry.lastValue) {
			callback(target, value, entry.lastValue);
			entry.lastValue = value;
		}
	};
	try {
		entry.observe();
	} catch (e) {
		console.error(e);
	}
	return () => {
		entry.observe = void 0;
		entry.lastValue = void 0;
	};
}
var gc = /* @__PURE__ */ new Set();
function deleteEntry(entry) {
	if (!gc.size) setTimeout(() => {
		for (const e of gc) if (!e.contexts || e.contexts.size === 0) entries.get(e.target).delete(e.key);
		gc.clear();
	});
	gc.add(entry);
}
function invalidateEntry(entry, options) {
	dispatch(entry);
	if (options.clearValue) {
		entry.value = void 0;
		entry.assertValue = void 0;
		entry.lastValue = void 0;
	}
	if (options.deleteEntry) {
		if (entry.deps) {
			for (const depEntry of entry.deps) depEntry.contexts.delete(entry);
			entry.deps = void 0;
		}
		if (entry.contexts) {
			for (const context of entry.contexts) context.deps.delete(entry);
			entry.contexts = void 0;
		}
		deleteEntry(entry);
	}
}
function invalidate(target, key, options = {}) {
	invalidateEntry(getEntry(target, key), options);
}
function invalidateAll(target, options = {}) {
	const targetMap = entries.get(target);
	if (targetMap) for (const entry of targetMap.values()) invalidateEntry(entry, options);
}
//#endregion
export { assert, get, getCurrentValue, getEntries, getEntry, invalidate, invalidateAll, observe, sync };
