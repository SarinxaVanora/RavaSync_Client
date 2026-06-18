import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_CSSStyleRule } from "./CSSStyleRule.js";
import { require_CSSStyleSheet } from "./CSSStyleSheet.js";
import { require_CSSGroupingRule } from "./CSSGroupingRule.js";
import { require_CSSConditionRule } from "./CSSConditionRule.js";
import { require_CSSMediaRule } from "./CSSMediaRule.js";
import { require_CSSSupportsRule } from "./CSSSupportsRule.js";
import { require_CSSKeyframeRule } from "./CSSKeyframeRule.js";
import { require_CSSKeyframesRule } from "./CSSKeyframesRule.js";
import { require_CSSStyleDeclaration } from "./CSSStyleDeclaration.js";
//#region node_modules/cssom/lib/clone.js
var require_clone = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSStyleSheet: require_CSSStyleSheet().CSSStyleSheet,
		CSSRule: require_CSSRule().CSSRule,
		CSSStyleRule: require_CSSStyleRule().CSSStyleRule,
		CSSGroupingRule: require_CSSGroupingRule().CSSGroupingRule,
		CSSConditionRule: require_CSSConditionRule().CSSConditionRule,
		CSSMediaRule: require_CSSMediaRule().CSSMediaRule,
		CSSSupportsRule: require_CSSSupportsRule().CSSSupportsRule,
		CSSStyleDeclaration: require_CSSStyleDeclaration().CSSStyleDeclaration,
		CSSKeyframeRule: require_CSSKeyframeRule().CSSKeyframeRule,
		CSSKeyframesRule: require_CSSKeyframesRule().CSSKeyframesRule
	};
	/**
	* Produces a deep copy of stylesheet — the instance variables of stylesheet are copied recursively.
	* @param {CSSStyleSheet|CSSOM.CSSStyleSheet} stylesheet
	* @nosideeffects
	* @return {CSSOM.CSSStyleSheet}
	*/
	CSSOM.clone = function clone(stylesheet) {
		var cloned = new CSSOM.CSSStyleSheet();
		var rules = stylesheet.cssRules;
		if (!rules) return cloned;
		for (var i = 0, rulesLength = rules.length; i < rulesLength; i++) {
			var rule = rules[i];
			var ruleClone = cloned.cssRules[i] = new rule.constructor();
			var style = rule.style;
			if (style) {
				var styleClone = ruleClone.style = new CSSOM.CSSStyleDeclaration();
				for (var j = 0, styleLength = style.length; j < styleLength; j++) {
					var name = styleClone[j] = style[j];
					styleClone[name] = style[name];
					styleClone._importants[name] = style.getPropertyPriority(name);
				}
				styleClone.length = style.length;
			}
			if (rule.hasOwnProperty("keyText")) ruleClone.keyText = rule.keyText;
			if (rule.hasOwnProperty("selectorText")) ruleClone.selectorText = rule.selectorText;
			if (rule.hasOwnProperty("mediaText")) ruleClone.mediaText = rule.mediaText;
			if (rule.hasOwnProperty("conditionText")) ruleClone.conditionText = rule.conditionText;
			if (rule.hasOwnProperty("cssRules")) ruleClone.cssRules = clone(rule).cssRules;
		}
		return cloned;
	};
	exports.clone = CSSOM.clone;
}));
//#endregion
export { require_clone };
