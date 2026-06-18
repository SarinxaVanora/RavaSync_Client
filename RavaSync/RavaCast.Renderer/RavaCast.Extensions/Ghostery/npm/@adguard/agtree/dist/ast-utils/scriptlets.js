import { isNull, isUndefined } from "../utils/type-guards.js";
import { QuoteUtils } from "../utils/quotes.js";
//#region node_modules/@adguard/agtree/dist/ast-utils/scriptlets.js
/**
* @file Utility functions for working with scriptlet nodes
*/
/**
* Get name of the scriptlet from the scriptlet node
*
* @param scriptletNode Scriptlet node to get name of
* @returns Name of the scriptlet
* @throws If the scriptlet is empty
*/
function getScriptletName(scriptletNode) {
	if (scriptletNode.children.length === 0) throw new Error("Empty scriptlet");
	return scriptletNode.children[0]?.value ?? "";
}
/**
* Transform the nth argument of the scriptlet node
*
* @param scriptletNode Scriptlet node to transform argument of
* @param index Index of the argument to transform (index 0 is the scriptlet name)
* @param transform Function to transform the argument
*/
function transformNthScriptletArgument(scriptletNode, index, transform) {
	const child = scriptletNode.children[index];
	if (!isUndefined(child)) {
		const transformed = transform(child?.value ?? null);
		if (isNull(transformed)) {
			scriptletNode.children[index] = null;
			return;
		}
		if (isNull(child)) {
			scriptletNode.children[index] = {
				type: "Value",
				value: transformed
			};
			return;
		}
		child.value = transformed;
	}
}
/**
* Transform all arguments of the scriptlet node
*
* @param scriptletNode Scriptlet node to transform arguments of
* @param transform Function to transform the arguments
*/
function transformAllScriptletArguments(scriptletNode, transform) {
	for (let i = 0; i < scriptletNode.children.length; i += 1) transformNthScriptletArgument(scriptletNode, i, transform);
}
/**
* Set name of the scriptlet.
* Modifies input `scriptletNode` if needed.
*
* @param scriptletNode Scriptlet node to set name of
* @param name Name to set
*/
function setScriptletName(scriptletNode, name) {
	transformNthScriptletArgument(scriptletNode, 0, () => name);
}
/**
* Set quote type of the scriptlet parameters
*
* @param scriptletNode Scriptlet node to set quote type of
* @param quoteType Preferred quote type
*/
function setScriptletQuoteType(scriptletNode, quoteType) {
	transformAllScriptletArguments(scriptletNode, (value) => QuoteUtils.setStringQuoteType(value ?? "", quoteType));
}
//#endregion
export { getScriptletName, setScriptletName, setScriptletQuoteType, transformAllScriptletArguments, transformNthScriptletArgument };
