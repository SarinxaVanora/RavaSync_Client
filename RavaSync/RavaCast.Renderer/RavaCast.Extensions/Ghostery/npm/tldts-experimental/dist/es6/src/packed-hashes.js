import fast_path_default from "../../../npm/tldts-core/dist/es6/src/lookup/fast-path.js";
import "../../../npm/tldts-core/dist/es6/index.js";
import hashes_default from "./data/hashes.js";
//#region node_modules/tldts-experimental/dist/es6/src/packed-hashes.js
/**
* Find `elt` in `arr` between indices `start` (included) and `end` (excluded)
* using a binary search algorithm.
*/
function binSearch(arr, elt, start, end) {
	if (start >= end) return false;
	let low = start;
	let high = end - 1;
	while (low <= high) {
		const mid = low + high >>> 1;
		const midVal = arr[mid];
		if (midVal < elt) low = mid + 1;
		else if (midVal > elt) high = mid - 1;
		else return true;
	}
	return false;
}
var BUFFER = new Uint32Array(20);
/**
* Iterate on hashes of labels from `hostname` backward (from last label to
* first label), stopping after `maximumNumberOfLabels` have been extracted and
* calling `cb` on each of them.
*
* The `maximumNumberOfLabels` argument is typically used to specify the number
* of labels seen in the longest public suffix. We do not need to check further
* in very long hostnames.
*/
function hashHostnameLabelsBackward(hostname, maximumNumberOfLabels) {
	let hash = 5381;
	let index = 0;
	for (let i = hostname.length - 1; i >= 0; i -= 1) {
		const code = hostname.charCodeAt(i);
		if (code === 46) {
			BUFFER[index << 1] = hash >>> 0;
			BUFFER[(index << 1) + 1] = i + 1;
			index += 1;
			if (index === maximumNumberOfLabels) return index;
		}
		hash = hash * 33 ^ code;
	}
	BUFFER[index << 1] = hash >>> 0;
	BUFFER[(index << 1) + 1] = 0;
	index += 1;
	return index;
}
/**
* Perform a public suffix lookup for `hostname` using the packed hashes
* data-structure. The `options` allows to specify if ICANN/PRIVATE sections
* should be considered. By default, both are.
*
*/
function suffixLookup(hostname, options, out) {
	if (fast_path_default(hostname, options, out)) return;
	const { allowIcannDomains, allowPrivateDomains } = options;
	let matchIndex = -1;
	let matchKind = 0;
	let matchLabels = 0;
	let index = 1;
	const numberOfHashes = hashHostnameLabelsBackward(hostname, hashes_default[0]);
	for (let label = 0; label < numberOfHashes; label += 1) {
		const hash = BUFFER[label << 1];
		const labelStart = BUFFER[(label << 1) + 1];
		let match = 0;
		if (allowIcannDomains) match = binSearch(hashes_default, hash, index + 1, index + hashes_default[index] + 1) ? 5 : 0;
		index += hashes_default[index] + 1;
		if (allowPrivateDomains && match === 0) match = binSearch(hashes_default, hash, index + 1, index + hashes_default[index] + 1) ? 6 : 0;
		index += hashes_default[index] + 1;
		if (allowIcannDomains && match === 0 && (matchKind & 4) === 0) match = binSearch(hashes_default, hash, index + 1, index + hashes_default[index] + 1) ? 17 : 0;
		index += hashes_default[index] + 1;
		if (allowPrivateDomains && match === 0 && (matchKind & 4) === 0) match = binSearch(hashes_default, hash, index + 1, index + hashes_default[index] + 1) ? 18 : 0;
		index += hashes_default[index] + 1;
		if (allowIcannDomains && match === 0 && (matchKind & 4) === 0 && matchLabels <= label) match = binSearch(hashes_default, hash, index + 1, index + hashes_default[index] + 1) ? 9 : 0;
		index += hashes_default[index] + 1;
		if (allowPrivateDomains && match === 0 && (matchKind & 4) === 0 && matchLabels <= label) match = binSearch(hashes_default, hash, index + 1, index + hashes_default[index] + 1) ? 10 : 0;
		index += hashes_default[index] + 1;
		if (match !== 0) {
			matchKind = match;
			matchLabels = label + ((match & 16) !== 0 ? 2 : 1);
			matchIndex = labelStart;
		}
	}
	out.isIcann = (matchKind & 1) !== 0;
	out.isPrivate = (matchKind & 2) !== 0;
	if (matchIndex === -1) {
		out.publicSuffix = numberOfHashes === 1 ? hostname : hostname.slice(BUFFER[1]);
		return;
	}
	if ((matchKind & 4) !== 0) {
		out.publicSuffix = hostname.slice(BUFFER[(matchLabels - 2 << 1) + 1]);
		return;
	}
	if ((matchKind & 16) !== 0) {
		if (matchLabels < numberOfHashes) {
			out.publicSuffix = hostname.slice(BUFFER[(matchLabels - 1 << 1) + 1]);
			return;
		}
		const parts = hostname.split(".");
		while (parts.length > matchLabels) parts.shift();
		out.publicSuffix = parts.join(".");
		return;
	}
	out.publicSuffix = hostname.slice(matchIndex);
}
//#endregion
export { suffixLookup as default };
