//#region node_modules/@ghostery/adblocker/dist/esm/queue-microtask.js
/**
* The MIT License (MIT)
*
* Copyright (c) Feross Aboukhadijeh
*
* Originally from: https://github.com/feross/queue-microtask
*/
var promise;
var queueMicrotask = typeof window !== "undefined" && typeof window.queueMicrotask === "function" ? (cb) => window.queueMicrotask(cb) : (cb) => (promise || (promise = Promise.resolve())).then(cb).catch((err) => setTimeout(() => {
	throw err;
}, 0));
//#endregion
export { queueMicrotask };
