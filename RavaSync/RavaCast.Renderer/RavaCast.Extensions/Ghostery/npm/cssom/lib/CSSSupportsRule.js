import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_CSSGroupingRule } from "./CSSGroupingRule.js";
import { require_CSSConditionRule } from "./CSSConditionRule.js";
//#region node_modules/cssom/lib/CSSSupportsRule.js
var require_CSSSupportsRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSRule: require_CSSRule().CSSRule,
		CSSGroupingRule: require_CSSGroupingRule().CSSGroupingRule,
		CSSConditionRule: require_CSSConditionRule().CSSConditionRule
	};
	/**
	* @constructor
	* @see https://drafts.csswg.org/css-conditional-3/#the-csssupportsrule-interface
	*/
	CSSOM.CSSSupportsRule = function CSSSupportsRule() {
		CSSOM.CSSConditionRule.call(this);
	};
	CSSOM.CSSSupportsRule.prototype = new CSSOM.CSSConditionRule();
	CSSOM.CSSSupportsRule.prototype.constructor = CSSOM.CSSSupportsRule;
	CSSOM.CSSSupportsRule.prototype.type = 12;
	Object.defineProperty(CSSOM.CSSSupportsRule.prototype, "cssText", { get: function() {
		var cssTexts = [];
		for (var i = 0, length = this.cssRules.length; i < length; i++) cssTexts.push(this.cssRules[i].cssText);
		return "@supports " + this.conditionText + " {" + cssTexts.join("") + "}";
	} });
	exports.CSSSupportsRule = CSSOM.CSSSupportsRule;
}));
//#endregion
export { require_CSSSupportsRule };
