import { __toESM } from "../../../../../virtual/_rolldown/runtime.js";
import { require_boolbase } from "../../../../boolbase/index.js";
import { isTraversal } from "../sort.js";
//#region node_modules/css-select/lib/esm/pseudo-selectors/subselects.js
var import_boolbase = /* @__PURE__ */ __toESM(require_boolbase());
/** Used as a placeholder for :has. Will be replaced with the actual element. */
var PLACEHOLDER_ELEMENT = {};
function ensureIsTag(next, adapter) {
	if (next === import_boolbase.default.falseFunc) return import_boolbase.default.falseFunc;
	return (elem) => adapter.isTag(elem) && next(elem);
}
function getNextSiblings(elem, adapter) {
	const siblings = adapter.getSiblings(elem);
	if (siblings.length <= 1) return [];
	const elemIndex = siblings.indexOf(elem);
	if (elemIndex < 0 || elemIndex === siblings.length - 1) return [];
	return siblings.slice(elemIndex + 1).filter(adapter.isTag);
}
function copyOptions(options) {
	return {
		xmlMode: !!options.xmlMode,
		lowerCaseAttributeNames: !!options.lowerCaseAttributeNames,
		lowerCaseTags: !!options.lowerCaseTags,
		quirksMode: !!options.quirksMode,
		cacheResults: !!options.cacheResults,
		pseudos: options.pseudos,
		adapter: options.adapter,
		equals: options.equals
	};
}
var is = (next, token, options, context, compileToken) => {
	const func = compileToken(token, copyOptions(options), context);
	return func === import_boolbase.default.trueFunc ? next : func === import_boolbase.default.falseFunc ? import_boolbase.default.falseFunc : (elem) => func(elem) && next(elem);
};
var subselects = {
	is,
	/**
	* `:matches` and `:where` are aliases for `:is`.
	*/
	matches: is,
	where: is,
	not(next, token, options, context, compileToken) {
		const func = compileToken(token, copyOptions(options), context);
		return func === import_boolbase.default.falseFunc ? next : func === import_boolbase.default.trueFunc ? import_boolbase.default.falseFunc : (elem) => !func(elem) && next(elem);
	},
	has(next, subselect, options, _context, compileToken) {
		const { adapter } = options;
		const opts = copyOptions(options);
		opts.relativeSelector = true;
		const context = subselect.some((s) => s.some(isTraversal)) ? [PLACEHOLDER_ELEMENT] : void 0;
		const compiled = compileToken(subselect, opts, context);
		if (compiled === import_boolbase.default.falseFunc) return import_boolbase.default.falseFunc;
		const hasElement = ensureIsTag(compiled, adapter);
		if (context && compiled !== import_boolbase.default.trueFunc) {
			const { shouldTestNextSiblings = false } = compiled;
			return (elem) => {
				if (!next(elem)) return false;
				context[0] = elem;
				const childs = adapter.getChildren(elem);
				const nextElements = shouldTestNextSiblings ? [...childs, ...getNextSiblings(elem, adapter)] : childs;
				return adapter.existsOne(hasElement, nextElements);
			};
		}
		return (elem) => next(elem) && adapter.existsOne(hasElement, adapter.getChildren(elem));
	}
};
//#endregion
export { PLACEHOLDER_ELEMENT, ensureIsTag, getNextSiblings, subselects };
