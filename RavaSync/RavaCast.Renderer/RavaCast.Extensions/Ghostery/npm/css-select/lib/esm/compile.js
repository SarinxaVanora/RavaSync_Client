import { __toESM } from "../../../../virtual/_rolldown/runtime.js";
import { require_boolbase } from "../../../boolbase/index.js";
import { SelectorType } from "../../../css-what/lib/es/types.js";
import { parse } from "../../../css-what/lib/es/parse.js";
import sortByProcedure, { isTraversal } from "./sort.js";
import { PLACEHOLDER_ELEMENT, ensureIsTag } from "./pseudo-selectors/subselects.js";
import { compileGeneralSelector } from "./general.js";
//#region node_modules/css-select/lib/esm/compile.js
var import_boolbase = /* @__PURE__ */ __toESM(require_boolbase());
/**
* Compiles a selector to an executable function.
*
* @param selector Selector to compile.
* @param options Compilation options.
* @param context Optional context for the selector.
*/
function compile(selector, options, context) {
	return ensureIsTag(compileUnsafe(selector, options, context), options.adapter);
}
function compileUnsafe(selector, options, context) {
	return compileToken(typeof selector === "string" ? parse(selector) : selector, options, context);
}
function includesScopePseudo(t) {
	return t.type === SelectorType.Pseudo && (t.name === "scope" || Array.isArray(t.data) && t.data.some((data) => data.some(includesScopePseudo)));
}
var DESCENDANT_TOKEN = { type: SelectorType.Descendant };
var FLEXIBLE_DESCENDANT_TOKEN = { type: "_flexibleDescendant" };
var SCOPE_TOKEN = {
	type: SelectorType.Pseudo,
	name: "scope",
	data: null
};
function absolutize(token, { adapter }, context) {
	const hasContext = !!(context === null || context === void 0 ? void 0 : context.every((e) => {
		const parent = adapter.isTag(e) && adapter.getParent(e);
		return e === PLACEHOLDER_ELEMENT || parent && adapter.isTag(parent);
	}));
	for (const t of token) {
		if (t.length > 0 && isTraversal(t[0]) && t[0].type !== SelectorType.Descendant) {} else if (hasContext && !t.some(includesScopePseudo)) t.unshift(DESCENDANT_TOKEN);
		else continue;
		t.unshift(SCOPE_TOKEN);
	}
}
function compileToken(token, options, context) {
	var _a;
	token.forEach(sortByProcedure);
	context = (_a = options.context) !== null && _a !== void 0 ? _a : context;
	const isArrayContext = Array.isArray(context);
	const finalContext = context && (Array.isArray(context) ? context : [context]);
	if (options.relativeSelector !== false) absolutize(token, options, finalContext);
	else if (token.some((t) => t.length > 0 && isTraversal(t[0]))) throw new Error("Relative selectors are not allowed when the `relativeSelector` option is disabled");
	let shouldTestNextSiblings = false;
	const query = token.map((rules) => {
		if (rules.length >= 2) {
			const [first, second] = rules;
			if (first.type !== SelectorType.Pseudo || first.name !== "scope") {} else if (isArrayContext && second.type === SelectorType.Descendant) rules[1] = FLEXIBLE_DESCENDANT_TOKEN;
			else if (second.type === SelectorType.Adjacent || second.type === SelectorType.Sibling) shouldTestNextSiblings = true;
		}
		return compileRules(rules, options, finalContext);
	}).reduce(reduceRules, import_boolbase.default.falseFunc);
	query.shouldTestNextSiblings = shouldTestNextSiblings;
	return query;
}
function compileRules(rules, options, context) {
	var _a;
	return rules.reduce((previous, rule) => previous === import_boolbase.default.falseFunc ? import_boolbase.default.falseFunc : compileGeneralSelector(previous, rule, options, context, compileToken), (_a = options.rootFunc) !== null && _a !== void 0 ? _a : import_boolbase.default.trueFunc);
}
function reduceRules(a, b) {
	if (b === import_boolbase.default.falseFunc || a === import_boolbase.default.trueFunc) return a;
	if (a === import_boolbase.default.falseFunc || b === import_boolbase.default.trueFunc) return b;
	return function combine(elem) {
		return a(elem) || b(elem);
	};
}
//#endregion
export { compile, compileToken, compileUnsafe };
