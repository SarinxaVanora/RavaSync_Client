import fast_path_default from "../../../../tldts-core/dist/es6/src/lookup/fast-path.js";
import "../../../../tldts-core/dist/es6/index.js";
import { exceptions, rules } from "./data/trie.js";
//#region node_modules/tldts/dist/es6/src/suffix-trie.js
/**
* Lookup parts of domain in Trie
*/
function lookupInTrie(parts, trie, index, allowedMask) {
	let result = null;
	let node = trie;
	while (node !== void 0) {
		if ((node.$ & allowedMask) !== 0) result = {
			index: index + 1,
			isIcann: node.$ === 1,
			isPrivate: node.$ === 2
		};
		if (index === -1) break;
		const succ = node.succ;
		node = succ && (succ[parts[index]] || succ["*"]);
		index -= 1;
	}
	return result;
}
/**
* Check if `hostname` has a valid public suffix in `trie`.
*/
function suffixLookup(hostname, options, out) {
	if (fast_path_default(hostname, options, out) === true) return;
	const hostnameParts = hostname.split(".");
	const allowedMask = (options.allowPrivateDomains === true ? 2 : 0) | (options.allowIcannDomains === true ? 1 : 0);
	const exceptionMatch = lookupInTrie(hostnameParts, exceptions, hostnameParts.length - 1, allowedMask);
	if (exceptionMatch !== null) {
		out.isIcann = exceptionMatch.isIcann;
		out.isPrivate = exceptionMatch.isPrivate;
		out.publicSuffix = hostnameParts.slice(exceptionMatch.index + 1).join(".");
		return;
	}
	const rulesMatch = lookupInTrie(hostnameParts, rules, hostnameParts.length - 1, allowedMask);
	if (rulesMatch !== null) {
		out.isIcann = rulesMatch.isIcann;
		out.isPrivate = rulesMatch.isPrivate;
		out.publicSuffix = hostnameParts.slice(rulesMatch.index).join(".");
		return;
	}
	out.isIcann = false;
	out.isPrivate = false;
	out.publicSuffix = hostnameParts[hostnameParts.length - 1];
}
//#endregion
export { suffixLookup as default };
