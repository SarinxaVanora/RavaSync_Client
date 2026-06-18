import { getEmptyResult, parseImpl } from "../../npm/tldts-core/dist/es6/src/factory.js";
import "../../npm/tldts-core/dist/es6/index.js";
import suffixLookup from "./src/packed-hashes.js";
getEmptyResult();
function parse(url, options = {}) {
	return parseImpl(url, 5, suffixLookup, options, getEmptyResult());
}
//#endregion
export { parse };
