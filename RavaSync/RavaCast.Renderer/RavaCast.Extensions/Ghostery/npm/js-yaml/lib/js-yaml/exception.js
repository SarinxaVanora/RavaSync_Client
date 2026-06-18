import { __commonJSMin } from "../../../../virtual/_rolldown/runtime.js";
//#region node_modules/js-yaml/lib/js-yaml/exception.js
var require_exception = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	function YAMLException(reason, mark) {
		Error.call(this);
		this.name = "YAMLException";
		this.reason = reason;
		this.mark = mark;
		this.message = (this.reason || "(unknown reason)") + (this.mark ? " " + this.mark.toString() : "");
		if (Error.captureStackTrace) Error.captureStackTrace(this, this.constructor);
		else this.stack = (/* @__PURE__ */ new Error()).stack || "";
	}
	YAMLException.prototype = Object.create(Error.prototype);
	YAMLException.prototype.constructor = YAMLException;
	YAMLException.prototype.toString = function toString(compact) {
		var result = this.name + ": ";
		result += this.reason || "(unknown reason)";
		if (!compact && this.mark) result += " " + this.mark.toString();
		return result;
	};
	module.exports = YAMLException;
}));
//#endregion
export { require_exception };
