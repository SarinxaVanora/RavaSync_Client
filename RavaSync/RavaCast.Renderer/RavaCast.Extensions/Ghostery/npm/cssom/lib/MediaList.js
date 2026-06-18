import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
//#region node_modules/cssom/lib/MediaList.js
var require_MediaList = /* @__PURE__ */ __commonJSMin(((exports) => {
	var CSSOM = {};
	/**
	* @constructor
	* @see http://dev.w3.org/csswg/cssom/#the-medialist-interface
	*/
	CSSOM.MediaList = function MediaList() {
		this.length = 0;
	};
	CSSOM.MediaList.prototype = {
		constructor: CSSOM.MediaList,
		/**
		* @return {string}
		*/
		get mediaText() {
			return Array.prototype.join.call(this, ", ");
		},
		/**
		* @param {string} value
		*/
		set mediaText(value) {
			var values = value.split(",");
			var length = this.length = values.length;
			for (var i = 0; i < length; i++) this[i] = values[i].trim();
		},
		/**
		* @param {string} medium
		*/
		appendMedium: function(medium) {
			if (Array.prototype.indexOf.call(this, medium) === -1) {
				this[this.length] = medium;
				this.length++;
			}
		},
		/**
		* @param {string} medium
		*/
		deleteMedium: function(medium) {
			var index = Array.prototype.indexOf.call(this, medium);
			if (index !== -1) Array.prototype.splice.call(this, index, 1);
		}
	};
	exports.MediaList = CSSOM.MediaList;
}));
//#endregion
export { require_MediaList };
