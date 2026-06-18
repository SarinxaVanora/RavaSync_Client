//#region node_modules/tldts-core/dist/es6/src/options.js
function setDefaultsImpl({ allowIcannDomains = true, allowPrivateDomains = false, detectIp = true, extractHostname = true, mixedInputs = true, validHosts = null, validateHostname = true }) {
	return {
		allowIcannDomains,
		allowPrivateDomains,
		detectIp,
		extractHostname,
		mixedInputs,
		validHosts,
		validateHostname
	};
}
var DEFAULT_OPTIONS = setDefaultsImpl({});
function setDefaults(options) {
	if (options === void 0) return DEFAULT_OPTIONS;
	return setDefaultsImpl(options);
}
//#endregion
export { setDefaults };
