import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_CSSGroupingRule } from "./CSSGroupingRule.js";
//#region node_modules/cssom/lib/CSSConditionRule.js
var require_CSSConditionRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSRule: require_CSSRule().CSSRule,
		CSSGroupingRule: require_CSSGroupingRule().CSSGroupingRule
	};
	/**
	* @constructor
	* @see https://www.w3.org/TR/css-conditional-3/#the-cssconditionrule-interface
	*/
	CSSOM.CSSConditionRule = function CSSConditionRule() {
		CSSOM.CSSGroupingRule.call(this);
		this.cssRules = [];
	};
	CSSOM.CSSConditionRule.prototype = new CSSOM.CSSGroupingRule();
	CSSOM.CSSConditionRule.prototype.constructor = CSSOM.CSSConditionRule;
	CSSOM.CSSConditionRule.prototype.conditionText = "";
	CSSOM.CSSConditionRule.prototype.cssText = "";
	exports.CSSConditionRule = CSSOM.CSSConditionRule;
}));
//#endregion
export { require_CSSConditionRule };
