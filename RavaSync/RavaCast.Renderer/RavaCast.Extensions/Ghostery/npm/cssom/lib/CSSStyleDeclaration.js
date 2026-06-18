import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_parse } from "./parse.js";
//#region node_modules/cssom/lib/CSSStyleDeclaration.js
var require_CSSStyleDeclaration = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {};
	/**
	* @constructor
	* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSStyleDeclaration
	*/
	CSSOM.CSSStyleDeclaration = function CSSStyleDeclaration() {
		this.length = 0;
		this.parentRule = null;
		this._importants = {};
	};
	CSSOM.CSSStyleDeclaration.prototype = {
		constructor: CSSOM.CSSStyleDeclaration,
		/**
		*
		* @param {string} name
		* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSStyleDeclaration-getPropertyValue
		* @return {string} the value of the property if it has been explicitly set for this declaration block.
		* Returns the empty string if the property has not been set.
		*/
		getPropertyValue: function(name) {
			return this[name] || "";
		},
		/**
		*
		* @param {string} name
		* @param {string} value
		* @param {string} [priority=null] "important" or null
		* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSStyleDeclaration-setProperty
		*/
		setProperty: function(name, value, priority) {
			if (this[name]) {
				if (Array.prototype.indexOf.call(this, name) < 0) {
					this[this.length] = name;
					this.length++;
				}
			} else {
				this[this.length] = name;
				this.length++;
			}
			this[name] = value + "";
			this._importants[name] = priority;
		},
		/**
		*
		* @param {string} name
		* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSStyleDeclaration-removeProperty
		* @return {string} the value of the property if it has been explicitly set for this declaration block.
		* Returns the empty string if the property has not been set or the property name does not correspond to a known CSS property.
		*/
		removeProperty: function(name) {
			if (!(name in this)) return "";
			var index = Array.prototype.indexOf.call(this, name);
			if (index < 0) return "";
			var prevValue = this[name];
			this[name] = "";
			Array.prototype.splice.call(this, index, 1);
			return prevValue;
		},
		getPropertyCSSValue: function() {},
		/**
		*
		* @param {String} name
		*/
		getPropertyPriority: function(name) {
			return this._importants[name] || "";
		},
		/**
		*   element.style.overflow = "auto"
		*   element.style.getPropertyShorthand("overflow-x")
		*   -> "overflow"
		*/
		getPropertyShorthand: function() {},
		isPropertyImplicit: function() {},
		get cssText() {
			var properties = [];
			for (var i = 0, length = this.length; i < length; ++i) {
				var name = this[i];
				var value = this.getPropertyValue(name);
				var priority = this.getPropertyPriority(name);
				if (priority) priority = " !" + priority;
				properties[i] = name + ": " + value + priority + ";";
			}
			return properties.join(" ");
		},
		set cssText(text) {
			var i, name;
			for (i = this.length; i--;) {
				name = this[i];
				this[name] = "";
			}
			Array.prototype.splice.call(this, 0, this.length);
			this._importants = {};
			var dummyRule = CSSOM.parse("#bogus{" + text + "}").cssRules[0].style;
			var length = dummyRule.length;
			for (i = 0; i < length; ++i) {
				name = dummyRule[i];
				this.setProperty(dummyRule[i], dummyRule.getPropertyValue(name), dummyRule.getPropertyPriority(name));
			}
		}
	};
	exports.CSSStyleDeclaration = CSSOM.CSSStyleDeclaration;
	CSSOM.parse = require_parse().parse;
}));
//#endregion
export { require_CSSStyleDeclaration };
