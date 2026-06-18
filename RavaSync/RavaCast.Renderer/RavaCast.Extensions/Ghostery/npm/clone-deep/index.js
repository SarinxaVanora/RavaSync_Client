import { __commonJSMin } from "../../virtual/_rolldown/runtime.js";
import { require_kind_of } from "../kind-of/index.js";
import { require_shallow_clone } from "../shallow-clone/index.js";
import { require_is_plain_object } from "../is-plain-object/index.js";
//#region node_modules/clone-deep/index.js
var require_clone_deep = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	/**
	* Module dependenices
	*/
	var clone = require_shallow_clone();
	var typeOf = require_kind_of();
	var isPlainObject = require_is_plain_object();
	function cloneDeep(val, instanceClone) {
		switch (typeOf(val)) {
			case "object": return cloneObjectDeep(val, instanceClone);
			case "array": return cloneArrayDeep(val, instanceClone);
			default: return clone(val);
		}
	}
	function cloneObjectDeep(val, instanceClone) {
		if (typeof instanceClone === "function") return instanceClone(val);
		if (instanceClone || isPlainObject(val)) {
			const res = new val.constructor();
			for (let key in val) res[key] = cloneDeep(val[key], instanceClone);
			return res;
		}
		return val;
	}
	function cloneArrayDeep(val, instanceClone) {
		const res = new val.constructor(val.length);
		for (let i = 0; i < val.length; i++) res[i] = cloneDeep(val[i], instanceClone);
		return res;
	}
	/**
	* Expose `cloneDeep`
	*/
	module.exports = cloneDeep;
}));
//#endregion
export { require_clone_deep };
