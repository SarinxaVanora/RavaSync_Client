import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
//#region node_modules/cssom/lib/CSSKeyframesRule.js
var require_CSSKeyframesRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = { CSSRule: require_CSSRule().CSSRule };
	/**
	* @constructor
	* @see http://www.w3.org/TR/css3-animations/#DOM-CSSKeyframesRule
	*/
	CSSOM.CSSKeyframesRule = function CSSKeyframesRule() {
		CSSOM.CSSRule.call(this);
		this.name = "";
		this.cssRules = [];
	};
	CSSOM.CSSKeyframesRule.prototype = new CSSOM.CSSRule();
	CSSOM.CSSKeyframesRule.prototype.constructor = CSSOM.CSSKeyframesRule;
	CSSOM.CSSKeyframesRule.prototype.type = 7;
	Object.defineProperty(CSSOM.CSSKeyframesRule.prototype, "cssText", { get: function() {
		var cssTexts = [];
		for (var i = 0, length = this.cssRules.length; i < length; i++) cssTexts.push("  " + this.cssRules[i].cssText);
		return "@" + (this._vendorPrefix || "") + "keyframes " + this.name + " { \n" + cssTexts.join("\n") + "\n}";
	} });
	exports.CSSKeyframesRule = CSSOM.CSSKeyframesRule;
}));
//#endregion
export { require_CSSKeyframesRule };
