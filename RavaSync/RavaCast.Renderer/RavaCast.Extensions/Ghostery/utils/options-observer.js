import store_default from "../npm/hybrids/src/store.js";
import Options from "../store/options.js";
//#region src/utils/options-observer.js
/**
* Ghostery Browser Extension
* https://www.ghostery.com/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
function isOptionEqual(a, b) {
	if (typeof b !== "object" || b === null) return a === b;
	const aKeys = Object.keys(a);
	const bKeys = Object.keys(b);
	return aKeys.length === bKeys.length && aKeys.every((key) => isOptionEqual(a[key], b[key]));
}
var observers = [];
var setup = null;
function addListener(...args) {
	if (setup === "done") throw new Error("The observer must be initialized synchronously");
	if (setup === null) setup = store_default.resolve(Options).then(() => {
		setup = "done";
	});
	return new Promise((resolve, reject) => {
		const fn = args[1] || args[0];
		const property = args.length === 2 ? args[0] : null;
		const getValue = property ? (v) => v[property] : (v) => v;
		const getLastValue = property ? (v) => v?.[property] : (v) => v;
		const wrapper = async (options, lastOptions) => {
			const value = getValue(options);
			const lastValue = getLastValue(lastOptions);
			if (property && isOptionEqual(value, lastValue)) return;
			try {
				console.debug(`[options] Executing "${fn.name || property}" observer`);
				await fn(value, lastValue);
				resolve();
			} catch (e) {
				reject(e);
				throw e;
			}
		};
		observers.push(wrapper);
	});
}
var queues = /* @__PURE__ */ new Set();
async function waitForIdle() {
	for (const queue of queues) await queue;
}
store_default.observe(Options, (_, options, lastOptions) => {
	if (observers.length === 0) return;
	const queue = Promise.allSettled([...queues]).then(async () => {
		console.debug(`[options] Start observers...`);
		await Promise.all(observers.map(async (fn) => {
			try {
				await fn(options, lastOptions);
			} catch (e) {
				console.error(`Error while executing observer: `, e);
			}
		}));
		console.debug(`[options] Observers finished...`);
		queues.delete(queue);
	});
	queues.add(queue);
});
//#endregion
export { addListener, isOptionEqual, waitForIdle };
