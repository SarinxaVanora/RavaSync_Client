import { __exportAll } from "../../../../virtual/_rolldown/runtime.js";
//#region node_modules/domelementtype/lib/esm/index.js
var esm_exports = /* @__PURE__ */ __exportAll({
	CDATA: () => CDATA,
	Comment: () => Comment,
	Directive: () => Directive,
	Doctype: () => Doctype,
	ElementType: () => ElementType,
	Root: () => Root,
	Script: () => Script,
	Style: () => Style,
	Tag: () => Tag,
	Text: () => Text,
	isTag: () => isTag
});
/** Types of elements found in htmlparser2's DOM */
var ElementType;
(function(ElementType) {
	/** Type for the root element of a document */
	ElementType["Root"] = "root";
	/** Type for Text */
	ElementType["Text"] = "text";
	/** Type for <? ... ?> */
	ElementType["Directive"] = "directive";
	/** Type for <!-- ... --> */
	ElementType["Comment"] = "comment";
	/** Type for <script> tags */
	ElementType["Script"] = "script";
	/** Type for <style> tags */
	ElementType["Style"] = "style";
	/** Type for Any tag */
	ElementType["Tag"] = "tag";
	/** Type for <![CDATA[ ... ]]> */
	ElementType["CDATA"] = "cdata";
	/** Type for <!doctype ...> */
	ElementType["Doctype"] = "doctype";
})(ElementType || (ElementType = {}));
/**
* Tests whether an element is a tag or not.
*
* @param elem Element to test
*/
function isTag(elem) {
	return elem.type === ElementType.Tag || elem.type === ElementType.Script || elem.type === ElementType.Style;
}
/** Type for the root element of a document */
var Root = ElementType.Root;
/** Type for Text */
var Text = ElementType.Text;
/** Type for <? ... ?> */
var Directive = ElementType.Directive;
/** Type for <!-- ... --> */
var Comment = ElementType.Comment;
/** Type for <script> tags */
var Script = ElementType.Script;
/** Type for <style> tags */
var Style = ElementType.Style;
/** Type for Any tag */
var Tag = ElementType.Tag;
/** Type for <![CDATA[ ... ]]> */
var CDATA = ElementType.CDATA;
/** Type for <!doctype ...> */
var Doctype = ElementType.Doctype;
//#endregion
export { CDATA, Comment, Directive, Doctype, ElementType, Root, Script, Style, Tag, Text, esm_exports, isTag };
