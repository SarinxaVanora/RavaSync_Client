//#region node_modules/tldts-core/dist/es6/src/lookup/fast-path.js
function fast_path_default(hostname, options, out) {
	if (options.allowPrivateDomains === false && hostname.length > 3) {
		const last = hostname.length - 1;
		const c3 = hostname.charCodeAt(last);
		const c2 = hostname.charCodeAt(last - 1);
		const c1 = hostname.charCodeAt(last - 2);
		const c0 = hostname.charCodeAt(last - 3);
		if (c3 === 109 && c2 === 111 && c1 === 99 && c0 === 46) {
			out.isIcann = true;
			out.isPrivate = false;
			out.publicSuffix = "com";
			return true;
		} else if (c3 === 103 && c2 === 114 && c1 === 111 && c0 === 46) {
			out.isIcann = true;
			out.isPrivate = false;
			out.publicSuffix = "org";
			return true;
		} else if (c3 === 117 && c2 === 100 && c1 === 101 && c0 === 46) {
			out.isIcann = true;
			out.isPrivate = false;
			out.publicSuffix = "edu";
			return true;
		} else if (c3 === 118 && c2 === 111 && c1 === 103 && c0 === 46) {
			out.isIcann = true;
			out.isPrivate = false;
			out.publicSuffix = "gov";
			return true;
		} else if (c3 === 116 && c2 === 101 && c1 === 110 && c0 === 46) {
			out.isIcann = true;
			out.isPrivate = false;
			out.publicSuffix = "net";
			return true;
		} else if (c3 === 101 && c2 === 100 && c1 === 46) {
			out.isIcann = true;
			out.isPrivate = false;
			out.publicSuffix = "de";
			return true;
		}
	}
	return false;
}
//#endregion
export { fast_path_default as default };
