import { GLOBAL_OBJ } from "./worldwide.js";
import { isNodeEnv } from "./node.js";
//#region node_modules/@sentry/core/build/esm/utils/isBrowser.js
/**
* Returns true if we are in the browser.
*/
function isBrowser() {
	return typeof window !== "undefined" && (!isNodeEnv() || isElectronNodeRenderer());
}
function isElectronNodeRenderer() {
	return GLOBAL_OBJ.process?.type === "renderer";
}
//#endregion
export { isBrowser };
