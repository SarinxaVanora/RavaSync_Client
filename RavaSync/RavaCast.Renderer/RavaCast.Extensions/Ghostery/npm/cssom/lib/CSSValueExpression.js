import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSValue } from "./CSSValue.js";
//#region node_modules/cssom/lib/CSSValueExpression.js
var require_CSSValueExpression = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = { CSSValue: require_CSSValue().CSSValue };
	/**
	* @constructor
	* @see http://msdn.microsoft.com/en-us/library/ms537634(v=vs.85).aspx
	*
	*/
	CSSOM.CSSValueExpression = function CSSValueExpression(token, idx) {
		this._token = token;
		this._idx = idx;
	};
	CSSOM.CSSValueExpression.prototype = new CSSOM.CSSValue();
	CSSOM.CSSValueExpression.prototype.constructor = CSSOM.CSSValueExpression;
	/**
	* parse css expression() value
	*
	* @return {Object}
	*         - error:
	*         or
	*         - idx:
	*         - expression:
	*
	* Example:
	*
	* .selector {
	*		zoom: expression(documentElement.clientWidth > 1000 ? '1000px' : 'auto');
	* }
	*/
	CSSOM.CSSValueExpression.prototype.parse = function() {
		var token = this._token, idx = this._idx;
		var character = "", expression = "", error = "", info, paren = [];
		for (;; ++idx) {
			character = token.charAt(idx);
			if (character === "") {
				error = "css expression error: unfinished expression!";
				break;
			}
			switch (character) {
				case "(":
					paren.push(character);
					expression += character;
					break;
				case ")":
					paren.pop(character);
					expression += character;
					break;
				case "/":
					if (info = this._parseJSComment(token, idx)) if (info.error) error = "css expression error: unfinished comment in expression!";
					else idx = info.idx;
					else if (info = this._parseJSRexExp(token, idx)) {
						idx = info.idx;
						expression += info.text;
					} else expression += character;
					break;
				case "'":
				case "\"":
					info = this._parseJSString(token, idx, character);
					if (info) {
						idx = info.idx;
						expression += info.text;
					} else expression += character;
					break;
				default:
					expression += character;
					break;
			}
			if (error) break;
			if (paren.length === 0) break;
		}
		var ret;
		if (error) ret = { error };
		else ret = {
			idx,
			expression
		};
		return ret;
	};
	/**
	*
	* @return {Object|false}
	*          - idx:
	*          - text:
	*          or
	*          - error:
	*          or
	*          false
	*
	*/
	CSSOM.CSSValueExpression.prototype._parseJSComment = function(token, idx) {
		var nextChar = token.charAt(idx + 1), text;
		if (nextChar === "/" || nextChar === "*") {
			var startIdx = idx, endIdx, commentEndChar;
			if (nextChar === "/") commentEndChar = "\n";
			else if (nextChar === "*") commentEndChar = "*/";
			endIdx = token.indexOf(commentEndChar, startIdx + 1 + 1);
			if (endIdx !== -1) {
				endIdx = endIdx + commentEndChar.length - 1;
				text = token.substring(idx, endIdx + 1);
				return {
					idx: endIdx,
					text
				};
			} else return { error: "css expression error: unfinished comment in expression!" };
		} else return false;
	};
	/**
	*
	* @return {Object|false}
	*					- idx:
	*					- text:
	*					or 
	*					false
	*
	*/
	CSSOM.CSSValueExpression.prototype._parseJSString = function(token, idx, sep) {
		var endIdx = this._findMatchedIdx(token, idx, sep), text;
		if (endIdx === -1) return false;
		else {
			text = token.substring(idx, endIdx + sep.length);
			return {
				idx: endIdx,
				text
			};
		}
	};
	/**
	* parse regexp in css expression
	*
	* @return {Object|false}
	*				- idx:
	*				- regExp:
	*				or 
	*				false
	*/
	CSSOM.CSSValueExpression.prototype._parseJSRexExp = function(token, idx) {
		var before = token.substring(0, idx).replace(/\s+$/, "");
		if (![
			/^$/,
			/\($/,
			/\[$/,
			/\!$/,
			/\+$/,
			/\-$/,
			/\*$/,
			/\/\s+/,
			/\%$/,
			/\=$/,
			/\>$/,
			/<$/,
			/\&$/,
			/\|$/,
			/\^$/,
			/\~$/,
			/\?$/,
			/\,$/,
			/delete$/,
			/in$/,
			/instanceof$/,
			/new$/,
			/typeof$/,
			/void$/
		].some(function(reg) {
			return reg.test(before);
		})) return false;
		else return this._parseJSString(token, idx, "/");
	};
	/**
	*
	* find next sep(same line) index in `token`
	*
	* @return {Number}
	*
	*/
	CSSOM.CSSValueExpression.prototype._findMatchedIdx = function(token, idx, sep) {
		var startIdx = idx, endIdx;
		var NOT_FOUND = -1;
		while (true) {
			endIdx = token.indexOf(sep, startIdx + 1);
			if (endIdx === -1) {
				endIdx = NOT_FOUND;
				break;
			} else {
				var matched = token.substring(idx + 1, endIdx).match(/\\+$/);
				if (!matched || matched[0] % 2 === 0) break;
				else startIdx = endIdx;
			}
		}
		if (token.indexOf("\n", idx + 1) < endIdx) endIdx = NOT_FOUND;
		return endIdx;
	};
	exports.CSSValueExpression = CSSOM.CSSValueExpression;
}));
//#endregion
export { require_CSSValueExpression };
