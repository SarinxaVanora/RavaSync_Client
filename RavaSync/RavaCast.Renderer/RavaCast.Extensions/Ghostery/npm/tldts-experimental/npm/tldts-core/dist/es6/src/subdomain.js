//#region node_modules/tldts-experimental/node_modules/tldts-core/dist/es6/src/subdomain.js
/**
* Returns the subdomain of a hostname string
*/
function getSubdomain(hostname, domain) {
	if (domain.length === hostname.length) return "";
	return hostname.slice(0, -domain.length - 1);
}
//#endregion
export { getSubdomain as default };
