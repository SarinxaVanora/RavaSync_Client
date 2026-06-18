import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_CSSStyleDeclaration } from "./CSSStyleDeclaration.js";
//#region node_modules/cssom/lib/CSSFontFaceRule.js
var require_CSSFontFaceRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSStyleDeclaration: require_CSSStyleDeclaration().CSSStyleDeclaration,
		CSSRule: require_CSSRule().CSSRule
	};
	/**
	* @constructor
	* @see http://dev.w3.org/csswg/cssom/#css-font-face-rule
	*/
	CSSOM.CSSFontFaceRule = function CSSFontFaceRule() {
		CSSOM.CSSRule.call(this);
		this.style = new CSSOM.CSSStyleDeclaration();
		this.style.parentRule = this;
	};
	CSSOM.CSSFontFaceRule.prototype = new CSSOM.CSSRule();
	CSSOM.CSSFontFaceRule.prototype.constructor = CSSOM.CSSFontFaceRule;
	CSSOM.CSSFontFaceRule.prototype.type = 5;
	Object.defineProperty(CSSOM.CSSFontFaceRule.prototype, "cssText", { get: function() {
		return "@font-face {" + this.style.cssText + "}";
	} });
	exports.CSSFontFaceRule = CSSOM.CSSFontFaceRule;
}));
//#endregion
export { require_CSSFontFaceRule };
