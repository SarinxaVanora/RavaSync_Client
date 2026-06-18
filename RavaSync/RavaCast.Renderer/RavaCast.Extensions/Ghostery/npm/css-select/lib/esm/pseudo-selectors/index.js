import { parse } from "../../../../css-what/lib/es/parse.js";
import { filters } from "./filters.js";
import { pseudos, verifyPseudoArgs } from "./pseudos.js";
import { aliases } from "./aliases.js";
import { subselects } from "./subselects.js";
//#region node_modules/css-select/lib/esm/pseudo-selectors/index.js
function compilePseudoSelector(next, selector, options, context, compileToken) {
	var _a;
	const { name, data } = selector;
	if (Array.isArray(data)) {
		if (!(name in subselects)) throw new Error(`Unknown pseudo-class :${name}(${data})`);
		return subselects[name](next, data, options, context, compileToken);
	}
	const userPseudo = (_a = options.pseudos) === null || _a === void 0 ? void 0 : _a[name];
	const stringPseudo = typeof userPseudo === "string" ? userPseudo : aliases[name];
	if (typeof stringPseudo === "string") {
		if (data != null) throw new Error(`Pseudo ${name} doesn't have any arguments`);
		const alias = parse(stringPseudo);
		return subselects["is"](next, alias, options, context, compileToken);
	}
	if (typeof userPseudo === "function") {
		verifyPseudoArgs(userPseudo, name, data, 1);
		return (elem) => userPseudo(elem, data) && next(elem);
	}
	if (name in filters) return filters[name](next, data, options, context);
	if (name in pseudos) {
		const pseudo = pseudos[name];
		verifyPseudoArgs(pseudo, name, data, 2);
		return (elem) => pseudo(elem, options, data) && next(elem);
	}
	throw new Error(`Unknown pseudo-class :${name}`);
}
//#endregion
export { compilePseudoSelector };
