import { __commonJSMin } from "../../virtual/_rolldown/runtime.js";
//#region node_modules/isobject/index.js
/*!
* isobject <https://github.com/jonschlinkert/isobject>
*
* Copyright (c) 2014-2017, Jon Schlinkert.
* Released under the MIT License.
*/
var require_isobject = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = function isObject(val) {
		return val != null && typeof val === "object" && Array.isArray(val) === false;
	};
}));
//#endregion
export { require_isobject };
