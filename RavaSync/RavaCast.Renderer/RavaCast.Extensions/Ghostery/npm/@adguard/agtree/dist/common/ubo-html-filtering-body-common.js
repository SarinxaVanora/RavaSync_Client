import "../utils/constants.js";
//#region node_modules/@adguard/agtree/dist/common/ubo-html-filtering-body-common.js
/**
* Checks whether the given HTML filtering rule body represents a uBlock-style response header removal rule.
*
* @param node Potential response header removal rule node.
*
* @returns `true` if the node is a response header removal rule, `false` otherwise.
*
* @note This method checks `HtmlFilteringRuleBody` because, response header
* removal rule syntax is same as uBlock-style HTML filtering rule syntax.
*/
function isUboResponseHeaderRemovalRuleBody(node) {
	const { selectorList } = node;
	if (selectorList.children.length !== 1) return false;
	const complexSelector = selectorList.children[0];
	if (complexSelector.children.length !== 1) return false;
	const simpleSelector = complexSelector.children[0];
	return simpleSelector.type === "PseudoClassSelector" && simpleSelector.name.value === "responseheader" && simpleSelector.argument !== void 0;
}
//#endregion
export { isUboResponseHeaderRemovalRuleBody };
