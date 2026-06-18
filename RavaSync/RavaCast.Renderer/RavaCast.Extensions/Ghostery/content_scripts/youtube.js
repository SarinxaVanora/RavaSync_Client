(function() {
	//#region src/utils/notifications.js
	/**
	* Ghostery Browser Extension
	* https://www.ghostery.com/
	*
	* Copyright 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at http://mozilla.org/MPL/2.0
	*/
	var OPEN_ACTION = "notifications:open";
	var CLOSE_ACTION = "notifications:close";
	//#endregion
	//#region src/content_scripts/youtube.js
	/**
	* Ghostery Browser Extension
	* https://www.ghostery.com/
	*
	* Copyright 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at http://mozilla.org/MPL/2.0
	*/
	var SELECTORS = [
		"ytd-watch-flexy:not([hidden]) ytd-enforcement-message-view-model > div.ytd-enforcement-message-view-model",
		"yt-playability-error-supported-renderers#error-screen ytd-enforcement-message-view-model",
		"tp-yt-paper-dialog .ytd-enforcement-message-view-model"
	];
	function detectWall(cb) {
		let timeout = null;
		const observer = new MutationObserver(() => {
			if (timeout) return;
			timeout = setTimeout(() => {
				if (document.querySelector(SELECTORS)?.clientHeight > 0) try {
					cb();
				} catch {}
				else timeout = null;
			}, 1e3);
		});
		document.addEventListener("yt-navigate-start", () => {
			clearTimeout(timeout);
			timeout = null;
		});
		document.addEventListener("DOMContentLoaded", () => {
			observer.observe(document.body, {
				childList: true,
				subtree: true,
				attributeFilter: ["src", "style"]
			});
		});
	}
	async function isFeatureDisabled() {
		const { options, youtubeDontAsk } = await chrome.storage.local.get(["options", "youtubeDontAsk"]);
		if (youtubeDontAsk || !options || !options.terms || !!options.paused["<all_urls>"] || !!options.paused["www.youtube.com"] || !!options.paused["youtube.com"]) return true;
		return false;
	}
	if (!chrome.extension.inIncognitoContext) (async () => {
		if (await isFeatureDisabled()) return;
		detectWall(async () => {
			if (await isFeatureDisabled()) return;
			chrome.runtime.sendMessage({
				action: OPEN_ACTION,
				id: "youtube",
				params: { url: window.location.href }
			});
		});
		window.addEventListener("yt-navigate-start", () => {
			chrome.runtime.sendMessage({ action: CLOSE_ACTION });
		}, true);
	})();
	//#endregion
})();
