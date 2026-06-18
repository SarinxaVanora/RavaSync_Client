//#region node_modules/tldts-core/dist/es6/src/extract-hostname.js
/**
* @param url - URL we want to extract a hostname from.
* @param urlIsValidHostname - hint from caller; true if `url` is already a valid hostname.
*/
function extractHostname(url, urlIsValidHostname) {
	let start = 0;
	let end = url.length;
	let hasUpper = false;
	if (urlIsValidHostname === false) {
		if (url.startsWith("data:") === true) return null;
		while (start < url.length && url.charCodeAt(start) <= 32) start += 1;
		while (end > start + 1 && url.charCodeAt(end - 1) <= 32) end -= 1;
		if (url.charCodeAt(start) === 47 && url.charCodeAt(start + 1) === 47) start += 2;
		else {
			const indexOfProtocol = url.indexOf(":/", start);
			if (indexOfProtocol !== -1) {
				const protocolSize = indexOfProtocol - start;
				const c0 = url.charCodeAt(start);
				const c1 = url.charCodeAt(start + 1);
				const c2 = url.charCodeAt(start + 2);
				const c3 = url.charCodeAt(start + 3);
				const c4 = url.charCodeAt(start + 4);
				if (protocolSize === 5 && c0 === 104 && c1 === 116 && c2 === 116 && c3 === 112 && c4 === 115) {} else if (protocolSize === 4 && c0 === 104 && c1 === 116 && c2 === 116 && c3 === 112) {} else if (protocolSize === 3 && c0 === 119 && c1 === 115 && c2 === 115) {} else if (protocolSize === 2 && c0 === 119 && c1 === 115) {} else for (let i = start; i < indexOfProtocol; i += 1) {
					const lowerCaseCode = url.charCodeAt(i) | 32;
					if ((lowerCaseCode >= 97 && lowerCaseCode <= 122 || lowerCaseCode >= 48 && lowerCaseCode <= 57 || lowerCaseCode === 46 || lowerCaseCode === 45 || lowerCaseCode === 43) === false) return null;
				}
				start = indexOfProtocol + 2;
				while (url.charCodeAt(start) === 47) start += 1;
			}
		}
		let indexOfIdentifier = -1;
		let indexOfClosingBracket = -1;
		let indexOfPort = -1;
		for (let i = start; i < end; i += 1) {
			const code = url.charCodeAt(i);
			if (code === 35 || code === 47 || code === 63) {
				end = i;
				break;
			} else if (code === 64) indexOfIdentifier = i;
			else if (code === 93) indexOfClosingBracket = i;
			else if (code === 58) indexOfPort = i;
			else if (code >= 65 && code <= 90) hasUpper = true;
		}
		if (indexOfIdentifier !== -1 && indexOfIdentifier > start && indexOfIdentifier < end) start = indexOfIdentifier + 1;
		if (url.charCodeAt(start) === 91) {
			if (indexOfClosingBracket !== -1) return url.slice(start + 1, indexOfClosingBracket).toLowerCase();
			return null;
		} else if (indexOfPort !== -1 && indexOfPort > start && indexOfPort < end) end = indexOfPort;
	}
	while (end > start + 1 && url.charCodeAt(end - 1) === 46) end -= 1;
	const hostname = start !== 0 || end !== url.length ? url.slice(start, end) : url;
	if (hasUpper) return hostname.toLowerCase();
	return hostname;
}
//#endregion
export { extractHostname as default };
