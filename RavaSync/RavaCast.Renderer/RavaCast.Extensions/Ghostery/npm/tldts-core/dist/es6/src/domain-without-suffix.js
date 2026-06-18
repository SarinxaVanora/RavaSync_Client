//#region node_modules/tldts-core/dist/es6/src/domain-without-suffix.js
/**
* Return the part of domain without suffix.
*
* Example: for domain 'foo.com', the result would be 'foo'.
*/
function getDomainWithoutSuffix(domain, suffix) {
	return domain.slice(0, -suffix.length - 1);
}
//#endregion
export { getDomainWithoutSuffix as default };
