import { __commonJSMin } from "../../../../../../virtual/_rolldown/runtime.js";
import { require_type } from "../../type.js";
//#region node_modules/js-yaml/lib/js-yaml/type/js/regexp.js
var require_regexp = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var Type = require_type();
	function resolveJavascriptRegExp(data) {
		if (data === null) return false;
		if (data.length === 0) return false;
		var regexp = data, tail = /\/([gim]*)$/.exec(data), modifiers = "";
		if (regexp[0] === "/") {
			if (tail) modifiers = tail[1];
			if (modifiers.length > 3) return false;
			if (regexp[regexp.length - modifiers.length - 1] !== "/") return false;
		}
		return true;
	}
	function constructJavascriptRegExp(data) {
		var regexp = data, tail = /\/([gim]*)$/.exec(data), modifiers = "";
		if (regexp[0] === "/") {
			if (tail) modifiers = tail[1];
			regexp = regexp.slice(1, regexp.length - modifiers.length - 1);
		}
		return new RegExp(regexp, modifiers);
	}
	function representJavascriptRegExp(object) {
		var result = "/" + object.source + "/";
		if (object.global) result += "g";
		if (object.multiline) result += "m";
		if (object.ignoreCase) result += "i";
		return result;
	}
	function isRegExp(object) {
		return Object.prototype.toString.call(object) === "[object RegExp]";
	}
	module.exports = new Type("tag:yaml.org,2002:js/regexp", {
		kind: "scalar",
		resolve: resolveJavascriptRegExp,
		construct: constructJavascriptRegExp,
		predicate: isRegExp,
		represent: representJavascriptRegExp
	});
}));
//#endregion
export { require_regexp };
