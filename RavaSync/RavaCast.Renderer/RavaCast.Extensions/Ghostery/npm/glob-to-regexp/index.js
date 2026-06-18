import { __commonJSMin } from "../../virtual/_rolldown/runtime.js";
//#region node_modules/glob-to-regexp/index.js
var require_glob_to_regexp = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = function(glob, opts) {
		if (typeof glob !== "string") throw new TypeError("Expected a string");
		var str = String(glob);
		var reStr = "";
		var extended = opts ? !!opts.extended : false;
		var globstar = opts ? !!opts.globstar : false;
		var inGroup = false;
		var flags = opts && typeof opts.flags === "string" ? opts.flags : "";
		var c;
		for (var i = 0, len = str.length; i < len; i++) {
			c = str[i];
			switch (c) {
				case "/":
				case "$":
				case "^":
				case "+":
				case ".":
				case "(":
				case ")":
				case "=":
				case "!":
				case "|":
					reStr += "\\" + c;
					break;
				case "?": if (extended) {
					reStr += ".";
					break;
				}
				case "[":
				case "]": if (extended) {
					reStr += c;
					break;
				}
				case "{": if (extended) {
					inGroup = true;
					reStr += "(";
					break;
				}
				case "}": if (extended) {
					inGroup = false;
					reStr += ")";
					break;
				}
				case ",":
					if (inGroup) {
						reStr += "|";
						break;
					}
					reStr += "\\" + c;
					break;
				case "*":
					var prevChar = str[i - 1];
					var starCount = 1;
					while (str[i + 1] === "*") {
						starCount++;
						i++;
					}
					var nextChar = str[i + 1];
					if (!globstar) reStr += ".*";
					else if (starCount > 1 && (prevChar === "/" || prevChar === void 0) && (nextChar === "/" || nextChar === void 0)) {
						reStr += "((?:[^/]*(?:/|$))*)";
						i++;
					} else reStr += "([^/]*)";
					break;
				default: reStr += c;
			}
		}
		if (!flags || !~flags.indexOf("g")) reStr = "^" + reStr + "$";
		return new RegExp(reStr, flags);
	};
}));
//#endregion
export { require_glob_to_regexp };
