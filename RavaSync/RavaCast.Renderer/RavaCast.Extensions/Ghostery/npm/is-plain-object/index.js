import { __commonJSMin } from "../../virtual/_rolldown/runtime.js";
import { require_isobject } from "../isobject/index.js";
//#region node_modules/is-plain-object/index.js
/*!
* is-plain-object <https://github.com/jonschlinkert/is-plain-object>
*
* Copyright (c) 2014-2017, Jon Schlinkert.
* Released under the MIT License.
*/
var require_is_plain_object = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var isObject = require_isobject();
	function isObjectObject(o) {
		return isObject(o) === true && Object.prototype.toString.call(o) === "[object Object]";
	}
	module.exports = function isPlainObject(o) {
		var ctor, prot;
		if (isObjectObject(o) === false) return false;
		ctor = o.constructor;
		if (typeof ctor !== "function") return false;
		prot = ctor.prototype;
		if (isObjectObject(prot) === false) return false;
		if (prot.hasOwnProperty("isPrototypeOf") === false) return false;
		return true;
	};
}));
//#endregion
export { require_is_plain_object };
