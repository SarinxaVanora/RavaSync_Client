import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSStyleRule } from "./CSSStyleRule.js";
import { require_CSSStyleSheet } from "./CSSStyleSheet.js";
import { require_CSSImportRule } from "./CSSImportRule.js";
import { require_CSSGroupingRule } from "./CSSGroupingRule.js";
import { require_CSSConditionRule } from "./CSSConditionRule.js";
import { require_CSSMediaRule } from "./CSSMediaRule.js";
import { require_CSSSupportsRule } from "./CSSSupportsRule.js";
import { require_CSSFontFaceRule } from "./CSSFontFaceRule.js";
import { require_CSSHostRule } from "./CSSHostRule.js";
import { require_CSSKeyframeRule } from "./CSSKeyframeRule.js";
import { require_CSSKeyframesRule } from "./CSSKeyframesRule.js";
import { require_CSSValueExpression } from "./CSSValueExpression.js";
import { require_CSSDocumentRule } from "./CSSDocumentRule.js";
import { require_CSSStyleDeclaration } from "./CSSStyleDeclaration.js";
//#region node_modules/cssom/lib/parse.js
var require_parse = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {};
	/**
	* @param {string} token
	*/
	CSSOM.parse = function parse(token) {
		var i = 0;
		/**
		"before-selector" or
		"selector" or
		"atRule" or
		"atBlock" or
		"conditionBlock" or
		"before-name" or
		"name" or
		"before-value" or
		"value"
		*/
		var state = "before-selector";
		var index;
		var buffer = "";
		var valueParenthesisDepth = 0;
		var SIGNIFICANT_WHITESPACE = {
			"selector": true,
			"value": true,
			"value-parenthesis": true,
			"atRule": true,
			"importRule-begin": true,
			"importRule": true,
			"atBlock": true,
			"conditionBlock": true,
			"documentRule-begin": true
		};
		var styleSheet = new CSSOM.CSSStyleSheet();
		var currentScope = styleSheet;
		var parentRule;
		var ancestorRules = [];
		var hasAncestors = false;
		var prevScope;
		var name, priority = "", styleRule, mediaRule, supportsRule, importRule, fontFaceRule, keyframesRule, documentRule, hostRule;
		var atKeyframesRegExp = /@(-(?:\w+-)+)?keyframes/g;
		var parseError = function(message) {
			var lines = token.substring(0, i).split("\n");
			var lineCount = lines.length;
			var charCount = lines.pop().length + 1;
			var error = /* @__PURE__ */ new Error(message + " (line " + lineCount + ", char " + charCount + ")");
			error.line = lineCount;
			error["char"] = charCount;
			error.styleSheet = styleSheet;
			throw error;
		};
		for (var character; character = token.charAt(i); i++) switch (character) {
			case " ":
			case "	":
			case "\r":
			case "\n":
			case "\f":
				if (SIGNIFICANT_WHITESPACE[state]) buffer += character;
				break;
			case "\"":
				index = i + 1;
				do {
					index = token.indexOf("\"", index) + 1;
					if (!index) parseError("Unmatched \"");
				} while (token[index - 2] === "\\");
				buffer += token.slice(i, index);
				i = index - 1;
				switch (state) {
					case "before-value":
						state = "value";
						break;
					case "importRule-begin":
						state = "importRule";
						break;
				}
				break;
			case "'":
				index = i + 1;
				do {
					index = token.indexOf("'", index) + 1;
					if (!index) parseError("Unmatched '");
				} while (token[index - 2] === "\\");
				buffer += token.slice(i, index);
				i = index - 1;
				switch (state) {
					case "before-value":
						state = "value";
						break;
					case "importRule-begin":
						state = "importRule";
						break;
				}
				break;
			case "/":
				if (token.charAt(i + 1) === "*") {
					i += 2;
					index = token.indexOf("*/", i);
					if (index === -1) parseError("Missing */");
					else i = index + 1;
				} else buffer += character;
				if (state === "importRule-begin") {
					buffer += " ";
					state = "importRule";
				}
				break;
			case "@":
				if (token.indexOf("@-moz-document", i) === i) {
					state = "documentRule-begin";
					documentRule = new CSSOM.CSSDocumentRule();
					documentRule.__starts = i;
					i += 13;
					buffer = "";
					break;
				} else if (token.indexOf("@media", i) === i) {
					state = "atBlock";
					mediaRule = new CSSOM.CSSMediaRule();
					mediaRule.__starts = i;
					i += 5;
					buffer = "";
					break;
				} else if (token.indexOf("@supports", i) === i) {
					state = "conditionBlock";
					supportsRule = new CSSOM.CSSSupportsRule();
					supportsRule.__starts = i;
					i += 8;
					buffer = "";
					break;
				} else if (token.indexOf("@host", i) === i) {
					state = "hostRule-begin";
					i += 4;
					hostRule = new CSSOM.CSSHostRule();
					hostRule.__starts = i;
					buffer = "";
					break;
				} else if (token.indexOf("@import", i) === i) {
					state = "importRule-begin";
					i += 6;
					buffer += "@import";
					break;
				} else if (token.indexOf("@font-face", i) === i) {
					state = "fontFaceRule-begin";
					i += 9;
					fontFaceRule = new CSSOM.CSSFontFaceRule();
					fontFaceRule.__starts = i;
					buffer = "";
					break;
				} else {
					atKeyframesRegExp.lastIndex = i;
					var matchKeyframes = atKeyframesRegExp.exec(token);
					if (matchKeyframes && matchKeyframes.index === i) {
						state = "keyframesRule-begin";
						keyframesRule = new CSSOM.CSSKeyframesRule();
						keyframesRule.__starts = i;
						keyframesRule._vendorPrefix = matchKeyframes[1];
						i += matchKeyframes[0].length - 1;
						buffer = "";
						break;
					} else if (state === "selector") state = "atRule";
				}
				buffer += character;
				break;
			case "{":
				if (state === "selector" || state === "atRule") {
					styleRule.selectorText = buffer.trim();
					styleRule.style.__starts = i;
					buffer = "";
					state = "before-name";
				} else if (state === "atBlock") {
					mediaRule.media.mediaText = buffer.trim();
					if (parentRule) ancestorRules.push(parentRule);
					currentScope = parentRule = mediaRule;
					mediaRule.parentStyleSheet = styleSheet;
					buffer = "";
					state = "before-selector";
				} else if (state === "conditionBlock") {
					supportsRule.conditionText = buffer.trim();
					if (parentRule) ancestorRules.push(parentRule);
					currentScope = parentRule = supportsRule;
					supportsRule.parentStyleSheet = styleSheet;
					buffer = "";
					state = "before-selector";
				} else if (state === "hostRule-begin") {
					if (parentRule) ancestorRules.push(parentRule);
					currentScope = parentRule = hostRule;
					hostRule.parentStyleSheet = styleSheet;
					buffer = "";
					state = "before-selector";
				} else if (state === "fontFaceRule-begin") {
					if (parentRule) fontFaceRule.parentRule = parentRule;
					fontFaceRule.parentStyleSheet = styleSheet;
					styleRule = fontFaceRule;
					buffer = "";
					state = "before-name";
				} else if (state === "keyframesRule-begin") {
					keyframesRule.name = buffer.trim();
					if (parentRule) {
						ancestorRules.push(parentRule);
						keyframesRule.parentRule = parentRule;
					}
					keyframesRule.parentStyleSheet = styleSheet;
					currentScope = parentRule = keyframesRule;
					buffer = "";
					state = "keyframeRule-begin";
				} else if (state === "keyframeRule-begin") {
					styleRule = new CSSOM.CSSKeyframeRule();
					styleRule.keyText = buffer.trim();
					styleRule.__starts = i;
					buffer = "";
					state = "before-name";
				} else if (state === "documentRule-begin") {
					documentRule.matcher.matcherText = buffer.trim();
					if (parentRule) {
						ancestorRules.push(parentRule);
						documentRule.parentRule = parentRule;
					}
					currentScope = parentRule = documentRule;
					documentRule.parentStyleSheet = styleSheet;
					buffer = "";
					state = "before-selector";
				}
				break;
			case ":":
				if (state === "name") {
					name = buffer.trim();
					buffer = "";
					state = "before-value";
				} else buffer += character;
				break;
			case "(":
				if (state === "value") if (buffer.trim() === "expression") {
					var info = new CSSOM.CSSValueExpression(token, i).parse();
					if (info.error) parseError(info.error);
					else {
						buffer += info.expression;
						i = info.idx;
					}
				} else {
					state = "value-parenthesis";
					valueParenthesisDepth = 1;
					buffer += character;
				}
				else if (state === "value-parenthesis") {
					valueParenthesisDepth++;
					buffer += character;
				} else buffer += character;
				break;
			case ")":
				if (state === "value-parenthesis") {
					valueParenthesisDepth--;
					if (valueParenthesisDepth === 0) state = "value";
				}
				buffer += character;
				break;
			case "!":
				if (state === "value" && token.indexOf("!important", i) === i) {
					priority = "important";
					i += 9;
				} else buffer += character;
				break;
			case ";":
				switch (state) {
					case "value":
						styleRule.style.setProperty(name, buffer.trim(), priority);
						priority = "";
						buffer = "";
						state = "before-name";
						break;
					case "atRule":
						buffer = "";
						state = "before-selector";
						break;
					case "importRule":
						importRule = new CSSOM.CSSImportRule();
						importRule.parentStyleSheet = importRule.styleSheet.parentStyleSheet = styleSheet;
						importRule.cssText = buffer + character;
						styleSheet.cssRules.push(importRule);
						buffer = "";
						state = "before-selector";
						break;
					default:
						buffer += character;
						break;
				}
				break;
			case "}":
				switch (state) {
					case "value":
						styleRule.style.setProperty(name, buffer.trim(), priority);
						priority = "";
					case "before-name":
					case "name":
						styleRule.__ends = i + 1;
						if (parentRule) styleRule.parentRule = parentRule;
						styleRule.parentStyleSheet = styleSheet;
						currentScope.cssRules.push(styleRule);
						buffer = "";
						if (currentScope.constructor === CSSOM.CSSKeyframesRule) state = "keyframeRule-begin";
						else state = "before-selector";
						break;
					case "keyframeRule-begin":
					case "before-selector":
					case "selector":
						if (!parentRule) parseError("Unexpected }");
						hasAncestors = ancestorRules.length > 0;
						while (ancestorRules.length > 0) {
							parentRule = ancestorRules.pop();
							if (parentRule.constructor.name === "CSSMediaRule" || parentRule.constructor.name === "CSSSupportsRule") {
								prevScope = currentScope;
								currentScope = parentRule;
								currentScope.cssRules.push(prevScope);
								break;
							}
							if (ancestorRules.length === 0) hasAncestors = false;
						}
						if (!hasAncestors) {
							currentScope.__ends = i + 1;
							styleSheet.cssRules.push(currentScope);
							currentScope = styleSheet;
							parentRule = null;
						}
						buffer = "";
						state = "before-selector";
						break;
				}
				break;
			default:
				switch (state) {
					case "before-selector":
						state = "selector";
						styleRule = new CSSOM.CSSStyleRule();
						styleRule.__starts = i;
						break;
					case "before-name":
						state = "name";
						break;
					case "before-value":
						state = "value";
						break;
					case "importRule-begin":
						state = "importRule";
						break;
				}
				buffer += character;
				break;
		}
		return styleSheet;
	};
	exports.parse = CSSOM.parse;
	CSSOM.CSSStyleSheet = require_CSSStyleSheet().CSSStyleSheet;
	CSSOM.CSSStyleRule = require_CSSStyleRule().CSSStyleRule;
	CSSOM.CSSImportRule = require_CSSImportRule().CSSImportRule;
	CSSOM.CSSGroupingRule = require_CSSGroupingRule().CSSGroupingRule;
	CSSOM.CSSMediaRule = require_CSSMediaRule().CSSMediaRule;
	CSSOM.CSSConditionRule = require_CSSConditionRule().CSSConditionRule;
	CSSOM.CSSSupportsRule = require_CSSSupportsRule().CSSSupportsRule;
	CSSOM.CSSFontFaceRule = require_CSSFontFaceRule().CSSFontFaceRule;
	CSSOM.CSSHostRule = require_CSSHostRule().CSSHostRule;
	CSSOM.CSSStyleDeclaration = require_CSSStyleDeclaration().CSSStyleDeclaration;
	CSSOM.CSSKeyframeRule = require_CSSKeyframeRule().CSSKeyframeRule;
	CSSOM.CSSKeyframesRule = require_CSSKeyframesRule().CSSKeyframesRule;
	CSSOM.CSSValueExpression = require_CSSValueExpression().CSSValueExpression;
	CSSOM.CSSDocumentRule = require_CSSDocumentRule().CSSDocumentRule;
}));
//#endregion
export { require_parse };
