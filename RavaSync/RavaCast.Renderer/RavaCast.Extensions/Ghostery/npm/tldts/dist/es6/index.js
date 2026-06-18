import { getEmptyResult, parseImpl, resetResult } from "../../../tldts-core/dist/es6/src/factory.js";
import "../../../tldts-core/dist/es6/index.js";
import suffixLookup from "./src/suffix-trie.js";
//#region node_modules/tldts/dist/es6/index.js
var RESULT = getEmptyResult();
function getHostname(url, options = {}) {
	resetResult(RESULT);
	return parseImpl(url, 0, suffixLookup, options, RESULT).hostname;
}
function getPublicSuffix(url, options = {}) {
	resetResult(RESULT);
	return parseImpl(url, 2, suffixLookup, options, RESULT).publicSuffix;
}
function getDomain(url, options = {}) {
	resetResult(RESULT);
	return parseImpl(url, 3, suffixLookup, options, RESULT).domain;
}
//#endregion
export { getDomain, getHostname, getPublicSuffix };
