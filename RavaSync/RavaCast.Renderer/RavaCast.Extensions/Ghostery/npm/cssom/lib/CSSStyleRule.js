import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_CSSStyleDeclaration } from "./CSSStyleDeclaration.js";
//#region node_modules/cssom/lib/CSSStyleRule.js
var require_CSSStyleRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSStyleDeclaration: require_CSSStyleDeclaration().CSSStyleDeclaration,
		CSSRule: require_CSSRule().CSSRule
	};
	/**
	* @constructor
	* @see http://dev.w3.org/csswg/cssom/#cssstylerule
	* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSStyleRule
	*/
	CSSOM.CSSStyleRule = function CSSStyleRule() {
		CSSOM.CSSRule.call(this);
		this.selectorText = "";
		this.style = new CSSOM.CSSStyleDeclaration();
		this.style.parentRule = this;
	};
	CSSOM.CSSStyleRule.prototype = new CSSOM.CSSRule();
	CSSOM.CSSStyleRule.prototype.constructor = CSSOM.CSSStyleRule;
	CSSOM.CSSStyleRule.prototype.type = 1;
	Object.defineProperty(CSSOM.CSSStyleRule.prototype, "cssText", {
		get: function() {
			var text;
			if (this.selectorText) text = this.selectorText + " {" + this.style.cssText + "}";
			else text = "";
			return text;
		},
		set: function(cssText) {
			var rule = CSSOM.CSSStyleRule.parse(cssText);
			this.style = rule.style;
			this.selectorText = rule.selectorText;
		}
	});
	/**
	* NON-STANDARD
	* lightweight version of parse.js.
	* @param {string} ruleText
	* @return CSSStyleRule
	*/
	CSSOM.CSSStyleRule.parse = function(ruleText) {
		var i = 0;
		var state = "selector";
		var index;
		var j = i;
		var buffer = "";
		var SIGNIFICANT_WHITESPACE = {
			"selector": true,
			"value": true
		};
		var styleRule = new CSSOM.CSSStyleRule();
		var name, priority = "";
		for (var character; character = ruleText.charAt(i); i++) switch (character) {
			case " ":
			case "	":
			case "\r":
			case "\n":
			case "\f":
				if (SIGNIFICANT_WHITESPACE[state]) switch (ruleText.charAt(i - 1)) {
					case " ":
					case "	":
					case "\r":
					case "\n":
					case "\f": break;
					default:
						buffer += " ";
						break;
				}
				break;
			case "\"":
				j = i + 1;
				index = ruleText.indexOf("\"", j) + 1;
				if (!index) throw "\" is missing";
				buffer += ruleText.slice(i, index);
				i = index - 1;
				break;
			case "'":
				j = i + 1;
				index = ruleText.indexOf("'", j) + 1;
				if (!index) throw "' is missing";
				buffer += ruleText.slice(i, index);
				i = index - 1;
				break;
			case "/":
				if (ruleText.charAt(i + 1) === "*") {
					i += 2;
					index = ruleText.indexOf("*/", i);
					if (index === -1) throw new SyntaxError("Missing */");
					else i = index + 1;
				} else buffer += character;
				break;
			case "{":
				if (state === "selector") {
					styleRule.selectorText = buffer.trim();
					buffer = "";
					state = "name";
				}
				break;
			case ":":
				if (state === "name") {
					name = buffer.trim();
					buffer = "";
					state = "value";
				} else buffer += character;
				break;
			case "!":
				if (state === "value" && ruleText.indexOf("!important", i) === i) {
					priority = "important";
					i += 9;
				} else buffer += character;
				break;
			case ";":
				if (state === "value") {
					styleRule.style.setProperty(name, buffer.trim(), priority);
					priority = "";
					buffer = "";
					state = "name";
				} else buffer += character;
				break;
			case "}":
				if (state === "value") {
					styleRule.style.setProperty(name, buffer.trim(), priority);
					priority = "";
					buffer = "";
				} else if (state === "name") break;
				else buffer += character;
				state = "selector";
				break;
			default:
				buffer += character;
				break;
		}
		return styleRule;
	};
	exports.CSSStyleRule = CSSOM.CSSStyleRule;
}));
//#endregion
export { require_CSSStyleRule };
