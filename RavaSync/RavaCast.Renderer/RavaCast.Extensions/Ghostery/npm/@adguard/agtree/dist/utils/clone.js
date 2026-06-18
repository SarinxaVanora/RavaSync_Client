import { __toESM } from "../../../../../virtual/_rolldown/runtime.js";
import { require_clone_deep } from "../../../../clone-deep/index.js";
//#region node_modules/@adguard/agtree/dist/utils/clone.js
var import_clone_deep = /* @__PURE__ */ __toESM(require_clone_deep(), 1);
/**
* @file Clone related utilities
*
* We should keep clone related functions in this file. Thus, we just provide
* a simple interface for cloning values, we use it across the AGTree project,
* and the implementation "under the hood" can be improved later, if needed.
*/
/**
* Clones an input value to avoid side effects. Use it only in justified cases,
* because it can impact performance negatively.
*
* @param value Value to clone
* @returns Cloned value
*/
function clone(value) {
	return (0, import_clone_deep.default)(value);
}
//#endregion
export { clone };
