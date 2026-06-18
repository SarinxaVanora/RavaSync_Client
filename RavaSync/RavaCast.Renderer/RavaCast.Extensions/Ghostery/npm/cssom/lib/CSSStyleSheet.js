import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_StyleSheet } from "./StyleSheet.js";
import { require_CSSStyleRule } from "./CSSStyleRule.js";
import { require_parse } from "./parse.js";
//#region node_modules/cssom/lib/CSSStyleSheet.js
var require_CSSStyleSheet = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		StyleSheet: require_StyleSheet().StyleSheet,
		CSSStyleRule: require_CSSStyleRule().CSSStyleRule
	};
	/**
	* @constructor
	* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSStyleSheet
	*/
	CSSOM.CSSStyleSheet = function CSSStyleSheet() {
		CSSOM.StyleSheet.call(this);
		this.cssRules = [];
	};
	CSSOM.CSSStyleSheet.prototype = new CSSOM.StyleSheet();
	CSSOM.CSSStyleSheet.prototype.constructor = CSSOM.CSSStyleSheet;
	/**
	* Used to insert a new rule into the style sheet. The new rule now becomes part of the cascade.
	*
	*   sheet = new Sheet("body {margin: 0}")
	*   sheet.toString()
	*   -> "body{margin:0;}"
	*   sheet.insertRule("img {border: none}", 0)
	*   -> 0
	*   sheet.toString()
	*   -> "img{border:none;}body{margin:0;}"
	*
	* @param {string} rule
	* @param {number} index
	* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSStyleSheet-insertRule
	* @return {number} The index within the style sheet's rule collection of the newly inserted rule.
	*/
	CSSOM.CSSStyleSheet.prototype.insertRule = function(rule, index) {
		if (index < 0 || index > this.cssRules.length) throw new RangeError("INDEX_SIZE_ERR");
		var cssRule = CSSOM.parse(rule).cssRules[0];
		cssRule.parentStyleSheet = this;
		this.cssRules.splice(index, 0, cssRule);
		return index;
	};
	/**
	* Used to delete a rule from the style sheet.
	*
	*   sheet = new Sheet("img{border:none} body{margin:0}")
	*   sheet.toString()
	*   -> "img{border:none;}body{margin:0;}"
	*   sheet.deleteRule(0)
	*   sheet.toString()
	*   -> "body{margin:0;}"
	*
	* @param {number} index within the style sheet's rule list of the rule to remove.
	* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSStyleSheet-deleteRule
	*/
	CSSOM.CSSStyleSheet.prototype.deleteRule = function(index) {
		if (index < 0 || index >= this.cssRules.length) throw new RangeError("INDEX_SIZE_ERR");
		this.cssRules.splice(index, 1);
	};
	/**
	* NON-STANDARD
	* @return {string} serialize stylesheet
	*/
	CSSOM.CSSStyleSheet.prototype.toString = function() {
		var result = "";
		var rules = this.cssRules;
		for (var i = 0; i < rules.length; i++) result += rules[i].cssText + "\n";
		return result;
	};
	exports.CSSStyleSheet = CSSOM.CSSStyleSheet;
	CSSOM.parse = require_parse().parse;
}));
//#endregion
export { require_CSSStyleSheet };
