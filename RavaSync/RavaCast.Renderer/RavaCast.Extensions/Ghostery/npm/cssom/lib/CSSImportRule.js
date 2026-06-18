import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_CSSRule } from "./CSSRule.js";
import { require_CSSStyleSheet } from "./CSSStyleSheet.js";
import { require_MediaList } from "./MediaList.js";
//#region node_modules/cssom/lib/CSSImportRule.js
var require_CSSImportRule = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {
		CSSRule: require_CSSRule().CSSRule,
		CSSStyleSheet: require_CSSStyleSheet().CSSStyleSheet,
		MediaList: require_MediaList().MediaList
	};
	/**
	* @constructor
	* @see http://dev.w3.org/csswg/cssom/#cssimportrule
	* @see http://www.w3.org/TR/DOM-Level-2-Style/css.html#CSS-CSSImportRule
	*/
	CSSOM.CSSImportRule = function CSSImportRule() {
		CSSOM.CSSRule.call(this);
		this.href = "";
		this.media = new CSSOM.MediaList();
		this.styleSheet = new CSSOM.CSSStyleSheet();
	};
	CSSOM.CSSImportRule.prototype = new CSSOM.CSSRule();
	CSSOM.CSSImportRule.prototype.constructor = CSSOM.CSSImportRule;
	CSSOM.CSSImportRule.prototype.type = 3;
	Object.defineProperty(CSSOM.CSSImportRule.prototype, "cssText", {
		get: function() {
			var mediaText = this.media.mediaText;
			return "@import url(" + this.href + ")" + (mediaText ? " " + mediaText : "") + ";";
		},
		set: function(cssText) {
			var i = 0;
			/**
			* @import url(partial.css) screen, handheld;
			*        ||               |
			*        after-import     media
			*         |
			*         url
			*/
			var state = "";
			var buffer = "";
			var index;
			for (var character; character = cssText.charAt(i); i++) switch (character) {
				case " ":
				case "	":
				case "\r":
				case "\n":
				case "\f":
					if (state === "after-import") state = "url";
					else buffer += character;
					break;
				case "@":
					if (!state && cssText.indexOf("@import", i) === i) {
						state = "after-import";
						i += 6;
						buffer = "";
					}
					break;
				case "u":
					if (state === "url" && cssText.indexOf("url(", i) === i) {
						index = cssText.indexOf(")", i + 1);
						if (index === -1) throw i + ": \")\" not found";
						i += 4;
						var url = cssText.slice(i, index);
						if (url[0] === url[url.length - 1]) {
							if (url[0] === "\"" || url[0] === "'") url = url.slice(1, -1);
						}
						this.href = url;
						i = index;
						state = "media";
					}
					break;
				case "\"":
					if (state === "url") {
						index = cssText.indexOf("\"", i + 1);
						if (!index) throw i + ": '\"' not found";
						this.href = cssText.slice(i + 1, index);
						i = index;
						state = "media";
					}
					break;
				case "'":
					if (state === "url") {
						index = cssText.indexOf("'", i + 1);
						if (!index) throw i + ": \"'\" not found";
						this.href = cssText.slice(i + 1, index);
						i = index;
						state = "media";
					}
					break;
				case ";":
					if (state === "media") {
						if (buffer) this.media.mediaText = buffer.trim();
					}
					break;
				default:
					if (state === "media") buffer += character;
					break;
			}
		}
	});
	exports.CSSImportRule = CSSOM.CSSImportRule;
}));
//#endregion
export { require_CSSImportRule };
