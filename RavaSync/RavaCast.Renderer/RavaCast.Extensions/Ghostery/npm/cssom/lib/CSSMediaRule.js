import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_MediaList } from "./MediaList.js";
import { require_CSSGroupingRule } from "./CSSGroupingRule.js";
import { require_CSSConditionRule } from "./CSSConditionRule.js";
//#region node_modules/cssom/lib/CSSMediaRule.js
var require_CSSMediaRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSRule: require_CSSRule().CSSRule,
		CSSGroupingRule: require_CSSGroupingRule().CSSGroupingRule,
		CSSConditionRule: require_CSSConditionRule().CSSConditionRule,
		MediaList: require_MediaList().MediaList
	};
	/**
	* @constructor
	* @see http://dev.w3.org/csswg/cssom/#cssmediarule
	* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSMediaRule
	*/
	CSSOM.CSSMediaRule = function CSSMediaRule() {
		CSSOM.CSSConditionRule.call(this);
		this.media = new CSSOM.MediaList();
	};
	CSSOM.CSSMediaRule.prototype = new CSSOM.CSSConditionRule();
	CSSOM.CSSMediaRule.prototype.constructor = CSSOM.CSSMediaRule;
	CSSOM.CSSMediaRule.prototype.type = 4;
	Object.defineProperties(CSSOM.CSSMediaRule.prototype, {
		"conditionText": {
			get: function() {
				return this.media.mediaText;
			},
			set: function(value) {
				this.media.mediaText = value;
			},
			configurable: true,
			enumerable: true
		},
		"cssText": {
			get: function() {
				var cssTexts = [];
				for (var i = 0, length = this.cssRules.length; i < length; i++) cssTexts.push(this.cssRules[i].cssText);
				return "@media " + this.media.mediaText + " {" + cssTexts.join("") + "}";
			},
			configurable: true,
			enumerable: true
		}
	});
	exports.CSSMediaRule = CSSOM.CSSMediaRule;
}));
//#endregion
export { require_CSSMediaRule };
