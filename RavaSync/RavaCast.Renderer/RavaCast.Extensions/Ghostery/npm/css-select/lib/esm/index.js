import "../../../../virtual/_rolldown/runtime.js";
import { esm_exports } from "../../../domutils/lib/esm/index.js";
import { require_boolbase } from "../../../boolbase/index.js";
import "./pseudo-selectors/filters.js";
import "./pseudo-selectors/pseudos.js";
import "./pseudo-selectors/aliases.js";
import "./pseudo-selectors/subselects.js";
import "./pseudo-selectors/index.js";
import { compile as compile$1 } from "./compile.js";
require_boolbase();
var defaultEquals = (a, b) => a === b;
var defaultOptions = {
	adapter: esm_exports,
	equals: defaultEquals
};
function convertOptionFormats(options) {
	var _a, _b, _c, _d;
	const opts = options !== null && options !== void 0 ? options : defaultOptions;
	(_a = opts.adapter) !== null && _a !== void 0 || (opts.adapter = esm_exports);
	(_b = opts.equals) !== null && _b !== void 0 || (opts.equals = (_d = (_c = opts.adapter) === null || _c === void 0 ? void 0 : _c.equals) !== null && _d !== void 0 ? _d : defaultEquals);
	return opts;
}
function wrapCompile(func) {
	return function addAdapter(selector, options, context) {
		return func(selector, convertOptionFormats(options), context);
	};
}
/**
* Compiles the query, returns a function.
*/
var compile = wrapCompile(compile$1);
/**
* Tests whether or not an element is matched by query.
*
* @template Node The generic Node type for the DOM adapter being used.
* @template ElementNode The Node type for elements for the DOM adapter being used.
* @param elem The element to test if it matches the query.
* @param query can be either a CSS selector string or a compiled query function.
* @param [options] options for querying the document.
* @see compile for supported selector queries.
* @returns
*/
function is(elem, query, options) {
	const opts = convertOptionFormats(options);
	return (typeof query === "function" ? query : compile$1(query, opts))(elem);
}
//#endregion
export { compile, is };
