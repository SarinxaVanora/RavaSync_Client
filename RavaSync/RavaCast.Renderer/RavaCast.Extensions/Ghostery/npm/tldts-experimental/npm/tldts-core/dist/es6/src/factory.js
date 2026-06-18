import getDomain from "./domain.js";
import getDomainWithoutSuffix from "./domain-without-suffix.js";
import extractHostname from "./extract-hostname.js";
import isIp from "./is-ip.js";
import is_valid_default from "./is-valid.js";
import { setDefaults } from "./options.js";
import getSubdomain from "./subdomain.js";
//#region node_modules/tldts-experimental/node_modules/tldts-core/dist/es6/src/factory.js
/**
* Implement a factory allowing to plug different implementations of suffix
* lookup (e.g.: using a trie or the packed hashes datastructures). This is used
* and exposed in `tldts.ts` and `tldts-experimental.ts` bundle entrypoints.
*/
function getEmptyResult() {
	return {
		domain: null,
		domainWithoutSuffix: null,
		hostname: null,
		isIcann: null,
		isIp: null,
		isPrivate: null,
		publicSuffix: null,
		subdomain: null
	};
}
function parseImpl(url, step, suffixLookup, partialOptions, result) {
	const options = setDefaults(partialOptions);
	if (typeof url !== "string") return result;
	if (!options.extractHostname) result.hostname = url;
	else if (options.mixedInputs) result.hostname = extractHostname(url, is_valid_default(url));
	else result.hostname = extractHostname(url, false);
	if (options.detectIp && result.hostname !== null) {
		result.isIp = isIp(result.hostname);
		if (result.isIp) return result;
	}
	if (options.validateHostname && options.extractHostname && result.hostname !== null && !is_valid_default(result.hostname)) {
		result.hostname = null;
		return result;
	}
	if (step === 0 || result.hostname === null) return result;
	suffixLookup(result.hostname, options, result);
	if (step === 2 || result.publicSuffix === null) return result;
	result.domain = getDomain(result.publicSuffix, result.hostname, options);
	if (step === 3 || result.domain === null) return result;
	result.subdomain = getSubdomain(result.hostname, result.domain);
	if (step === 4) return result;
	result.domainWithoutSuffix = getDomainWithoutSuffix(result.domain, result.publicSuffix);
	return result;
}
//#endregion
export { getEmptyResult, parseImpl };
