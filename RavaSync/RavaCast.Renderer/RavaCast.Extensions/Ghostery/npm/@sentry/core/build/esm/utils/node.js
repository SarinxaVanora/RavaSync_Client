import { isBrowserBundle } from "./env.js";
//#region node_modules/@sentry/core/build/esm/utils/node.js
/**
* NOTE: In order to avoid circular dependencies, if you add a function to this module and it needs to print something,
* you must either a) use `console.log` rather than the `debug` singleton, or b) put your function elsewhere.
*/
/**
* Checks whether we're in the Node.js or Browser environment
*
* @returns Answer to given question
*/
function isNodeEnv() {
	return !isBrowserBundle() && Object.prototype.toString.call(typeof process !== "undefined" ? process : 0) === "[object process]";
}
//#endregion
export { isNodeEnv };
