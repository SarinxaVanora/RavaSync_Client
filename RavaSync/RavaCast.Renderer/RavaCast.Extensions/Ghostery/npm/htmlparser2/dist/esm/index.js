import { __exportAll } from "../../../../virtual/_rolldown/runtime.js";
import { esm_exports as esm_exports$1 } from "../../../domelementtype/lib/esm/index.js";
import DomHandler from "../../../domhandler/lib/esm/index.js";
import { getFeed } from "../../../domutils/lib/esm/feeds.js";
import { esm_exports as esm_exports$2 } from "../../../domutils/lib/esm/index.js";
import Tokenizer, { QuoteType } from "./Tokenizer.js";
import { Parser } from "./Parser.js";
//#region node_modules/htmlparser2/dist/esm/index.js
var esm_exports = /* @__PURE__ */ __exportAll({
	DefaultHandler: () => DomHandler,
	DomHandler: () => DomHandler,
	DomUtils: () => esm_exports$2,
	ElementType: () => esm_exports$1,
	Parser: () => Parser,
	QuoteType: () => QuoteType,
	Tokenizer: () => Tokenizer,
	createDocumentStream: () => createDocumentStream,
	createDomStream: () => createDomStream,
	getFeed: () => getFeed,
	parseDOM: () => parseDOM,
	parseDocument: () => parseDocument,
	parseFeed: () => parseFeed
});
/**
* Parses the data, returns the resulting document.
*
* @param data The data that should be parsed.
* @param options Optional options for the parser and DOM handler.
*/
function parseDocument(data, options) {
	const handler = new DomHandler(void 0, options);
	new Parser(handler, options).end(data);
	return handler.root;
}
/**
* Parses data, returns an array of the root nodes.
*
* Note that the root nodes still have a `Document` node as their parent.
* Use `parseDocument` to get the `Document` node instead.
*
* @param data The data that should be parsed.
* @param options Optional options for the parser and DOM handler.
* @deprecated Use `parseDocument` instead.
*/
function parseDOM(data, options) {
	return parseDocument(data, options).children;
}
/**
* Creates a parser instance, with an attached DOM handler.
*
* @param callback A callback that will be called once parsing has been completed, with the resulting document.
* @param options Optional options for the parser and DOM handler.
* @param elementCallback An optional callback that will be called every time a tag has been completed inside of the DOM.
*/
function createDocumentStream(callback, options, elementCallback) {
	const handler = new DomHandler((error) => callback(error, handler.root), options, elementCallback);
	return new Parser(handler, options);
}
/**
* Creates a parser instance, with an attached DOM handler.
*
* @param callback A callback that will be called once parsing has been completed, with an array of root nodes.
* @param options Optional options for the parser and DOM handler.
* @param elementCallback An optional callback that will be called every time a tag has been completed inside of the DOM.
* @deprecated Use `createDocumentStream` instead.
*/
function createDomStream(callback, options, elementCallback) {
	return new Parser(new DomHandler(callback, options, elementCallback), options);
}
var parseFeedDefaultOptions = { xmlMode: true };
/**
* Parse a feed.
*
* @param feed The feed that should be parsed, as a string.
* @param options Optionally, options for parsing. When using this, you should set `xmlMode` to `true`.
*/
function parseFeed(feed, options = parseFeedDefaultOptions) {
	return getFeed(parseDOM(feed, options));
}
//#endregion
export { esm_exports };
