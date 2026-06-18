import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
//#region node_modules/cssom/lib/CSSHostRule.js
var require_CSSHostRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = { CSSRule: require_CSSRule().CSSRule };
	/**
	* @constructor
	* @see http://www.w3.org/TR/shadow-dom/#host-at-rule
	*/
	CSSOM.CSSHostRule = function CSSHostRule() {
		CSSOM.CSSRule.call(this);
		this.cssRules = [];
	};
	CSSOM.CSSHostRule.prototype = new CSSOM.CSSRule();
	CSSOM.CSSHostRule.prototype.constructor = CSSOM.CSSHostRule;
	CSSOM.CSSHostRule.prototype.type = 1001;
	Object.defineProperty(CSSOM.CSSHostRule.prototype, "cssText", { get: function() {
		var cssTexts = [];
		for (var i = 0, length = this.cssRules.length; i < length; i++) cssTexts.push(this.cssRules[i].cssText);
		return "@host {" + cssTexts.join("") + "}";
	} });
	exports.CSSHostRule = CSSOM.CSSHostRule;
}));
//#endregion
export { require_CSSHostRule };
