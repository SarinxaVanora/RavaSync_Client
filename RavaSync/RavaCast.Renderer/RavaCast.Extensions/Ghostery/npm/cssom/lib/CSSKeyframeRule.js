import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_CSSStyleDeclaration } from "./CSSStyleDeclaration.js";
//#region node_modules/cssom/lib/CSSKeyframeRule.js
var require_CSSKeyframeRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSRule: require_CSSRule().CSSRule,
		CSSStyleDeclaration: require_CSSStyleDeclaration().CSSStyleDeclaration
	};
	/**
	* @constructor
	* @see http://www.w3.org/TR/css3-animations/#DOM-CSSKeyframeRule
	*/
	CSSOM.CSSKeyframeRule = function CSSKeyframeRule() {
		CSSOM.CSSRule.call(this);
		this.keyText = "";
		this.style = new CSSOM.CSSStyleDeclaration();
		this.style.parentRule = this;
	};
	CSSOM.CSSKeyframeRule.prototype = new CSSOM.CSSRule();
	CSSOM.CSSKeyframeRule.prototype.constructor = CSSOM.CSSKeyframeRule;
	CSSOM.CSSKeyframeRule.prototype.type = 8;
	Object.defineProperty(CSSOM.CSSKeyframeRule.prototype, "cssText", { get: function() {
		return this.keyText + " {" + this.style.cssText + "} ";
	} });
	exports.CSSKeyframeRule = CSSOM.CSSKeyframeRule;
}));
//#endregion
export { require_CSSKeyframeRule };
