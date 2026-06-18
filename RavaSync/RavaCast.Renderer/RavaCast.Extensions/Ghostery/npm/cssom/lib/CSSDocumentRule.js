import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_MatcherList } from "./MatcherList.js";
//#region node_modules/cssom/lib/CSSDocumentRule.js
var require_CSSDocumentRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSRule: require_CSSRule().CSSRule,
		MatcherList: require_MatcherList().MatcherList
	};
	/**
	* @constructor
	* @see https://developer.mozilla.org/en/CSS/@-moz-document
	*/
	CSSOM.CSSDocumentRule = function CSSDocumentRule() {
		CSSOM.CSSRule.call(this);
		this.matcher = new CSSOM.MatcherList();
		this.cssRules = [];
	};
	CSSOM.CSSDocumentRule.prototype = new CSSOM.CSSRule();
	CSSOM.CSSDocumentRule.prototype.constructor = CSSOM.CSSDocumentRule;
	CSSOM.CSSDocumentRule.prototype.type = 10;
	Object.defineProperty(CSSOM.CSSDocumentRule.prototype, "cssText", { get: function() {
		var cssTexts = [];
		for (var i = 0, length = this.cssRules.length; i < length; i++) cssTexts.push(this.cssRules[i].cssText);
		return "@-moz-document " + this.matcher.matcherText + " {" + cssTexts.join("") + "}";
	} });
	exports.CSSDocumentRule = CSSOM.CSSDocumentRule;
}));
//#endregion
export { require_CSSDocumentRule };
