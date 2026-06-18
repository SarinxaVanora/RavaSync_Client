import { __commonJSMin } from "../../../../../../virtual/_rolldown/runtime.js";
import { require_type } from "../../type.js";
//#region node_modules/js-yaml/lib/js-yaml/type/js/undefined.js
var require_undefined = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var Type = require_type();
	function resolveJavascriptUndefined() {
		return true;
	}
	function constructJavascriptUndefined() {}
	function representJavascriptUndefined() {
		return "";
	}
	function isUndefined(object) {
		return typeof object === "undefined";
	}
	module.exports = new Type("tag:yaml.org,2002:js/undefined", {
		kind: "scalar",
		resolve: resolveJavascriptUndefined,
		construct: constructJavascriptUndefined,
		predicate: isUndefined,
		represent: representJavascriptUndefined
	});
}));
//#endregion
export { require_undefined };
