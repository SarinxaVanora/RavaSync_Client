import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
//#region node_modules/cssom/lib/CSSValue.js
var require_CSSValue = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {};
	/**
	* @constructor
	* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSValue
	*
	* TODO: add if needed
	*/
	CSSOM.CSSValue = function CSSValue() {};
	CSSOM.CSSValue.prototype = {
		constructor: CSSOM.CSSValue,
		set cssText(text) {
			var name = this._getConstructorName();
			throw new Error("DOMException: property \"cssText\" of \"" + name + "\" is readonly and can not be replaced with \"" + text + "\"!");
		},
		get cssText() {
			var name = this._getConstructorName();
			throw new Error("getter \"cssText\" of \"" + name + "\" is not implemented!");
		},
		_getConstructorName: function() {
			return this.constructor.toString().match(/function\s([^\(]+)/)[1];
		}
	};
	exports.CSSValue = CSSOM.CSSValue;
}));
//#endregion
export { require_CSSValue };
