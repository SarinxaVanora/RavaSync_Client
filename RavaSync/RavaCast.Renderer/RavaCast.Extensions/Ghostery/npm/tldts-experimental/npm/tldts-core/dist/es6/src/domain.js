//#region node_modules/tldts-experimental/node_modules/tldts-core/dist/es6/src/domain.js
/**
* Check if `vhost` is a valid suffix of `hostname` (top-domain)
*
* It means that `vhost` needs to be a suffix of `hostname` and we then need to
* make sure that: either they are equal, or the character preceding `vhost` in
* `hostname` is a '.' (it should not be a partial label).
*
* * hostname = 'not.evil.com' and vhost = 'vil.com'      => not ok
* * hostname = 'not.evil.com' and vhost = 'evil.com'     => ok
* * hostname = 'not.evil.com' and vhost = 'not.evil.com' => ok
*/
function shareSameDomainSuffix(hostname, vhost) {
	if (hostname.endsWith(vhost)) return hostname.length === vhost.length || hostname[hostname.length - vhost.length - 1] === ".";
	return false;
}
/**
* Given a hostname and its public suffix, extract the general domain.
*/
function extractDomainWithSuffix(hostname, publicSuffix) {
	const publicSuffixIndex = hostname.length - publicSuffix.length - 2;
	const lastDotBeforeSuffixIndex = hostname.lastIndexOf(".", publicSuffixIndex);
	if (lastDotBeforeSuffixIndex === -1) return hostname;
	return hostname.slice(lastDotBeforeSuffixIndex + 1);
}
/**
* Detects the domain based on rules and upon and a host string
*/
function getDomain(suffix, hostname, options) {
	if (options.validHosts !== null) {
		const validHosts = options.validHosts;
		for (const vhost of validHosts) if (shareSameDomainSuffix(hostname, vhost)) return vhost;
	}
	let numberOfLeadingDots = 0;
	if (hostname.startsWith(".")) while (numberOfLeadingDots < hostname.length && hostname[numberOfLeadingDots] === ".") numberOfLeadingDots += 1;
	if (suffix.length === hostname.length - numberOfLeadingDots) return null;
	return extractDomainWithSuffix(hostname, suffix);
}
//#endregion
export { getDomain as default };
