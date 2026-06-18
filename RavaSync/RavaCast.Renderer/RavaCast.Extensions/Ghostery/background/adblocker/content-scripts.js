(() => {
	const map = /* @__PURE__ */ new Map();
	return {
		async register(hostname, code) {
			this.unregister(hostname);
			try {
				const contentScript = await browser.contentScripts.register({
					js: [{ code }],
					allFrames: true,
					matches: [`https://*.${hostname}/*`, `http://*.${hostname}/*`],
					matchAboutBlank: true,
					matchOriginAsFallback: true,
					runAt: "document_start",
					world: "MAIN"
				});
				map.set(hostname, contentScript);
			} catch (e) {
				console.warn(e);
				this.unregister(hostname);
			}
		},
		isRegistered(hostname) {
			return map.has(hostname);
		},
		unregister(hostname) {
			const contentScript = map.get(hostname);
			if (contentScript) {
				contentScript.unregister();
				map.delete(hostname);
			}
		},
		unregisterAll() {
			for (const hostname of map.keys()) this.unregister(hostname);
		}
	};
})();
//#endregion
