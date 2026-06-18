//#region node_modules/linkedom/esm/shared/text-escaper.js
var { replace } = "";
var ca = /[<>&\xA0]/g;
var esca = {
	"\xA0": "&#160;",
	"&": "&amp;",
	"<": "&lt;",
	">": "&gt;"
};
var pe = (m) => esca[m];
/**
* Safely escape HTML entities such as `&`, `<`, `>` only.
* @param {string} es the input to safely escape
* @returns {string} the escaped input, and it **throws** an error if
*  the input type is unexpected, except for boolean and numbers,
*  converted as string.
*/
var escape = (es) => replace.call(es, ca, pe);
//#endregion
export { escape };
