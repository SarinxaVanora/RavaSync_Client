//#region node_modules/tldts-experimental/node_modules/tldts-core/dist/es6/src/is-valid.js
/**
* Implements fast shallow verification of hostnames. This does not perform a
* struct check on the content of labels (classes of Unicode characters, etc.)
* but instead check that the structure is valid (number of labels, length of
* labels, etc.).
*
* If you need stricter validation, consider using an external library.
*/
function isValidAscii(code) {
	return code >= 97 && code <= 122 || code >= 48 && code <= 57 || code > 127;
}
/**
* Check if a hostname string is valid. It's usually a preliminary check before
* trying to use getDomain or anything else.
*
* Beware: it does not check if the TLD exists.
*/
function is_valid_default(hostname) {
	if (hostname.length > 255) return false;
	if (hostname.length === 0) return false;
	if (!isValidAscii(hostname.charCodeAt(0)) && hostname.charCodeAt(0) !== 46 && hostname.charCodeAt(0) !== 95) return false;
	let lastDotIndex = -1;
	let lastCharCode = -1;
	const len = hostname.length;
	for (let i = 0; i < len; i += 1) {
		const code = hostname.charCodeAt(i);
		if (code === 46) {
			if (i - lastDotIndex > 64 || lastCharCode === 46 || lastCharCode === 45 || lastCharCode === 95) return false;
			lastDotIndex = i;
		} else if (!(isValidAscii(code) || code === 45 || code === 95)) return false;
		lastCharCode = code;
	}
	return len - lastDotIndex - 1 <= 63 && lastCharCode !== 45;
}
//#endregion
export { is_valid_default as default };
