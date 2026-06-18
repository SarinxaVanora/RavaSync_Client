(function() {
	//#region node_modules/@whotracksme/reporting/reporting/src/request/content-script.js
	/**
	* WhoTracks.Me
	* https://whotracks.me/
	*
	* Copyright 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at http://mozilla.org/MPL/2.0
	*/
	function recordMouseDown(ev) {
		function getContextHTML(ev) {
			let target = ev.target;
			let html;
			try {
				for (let count = 0; count < 5; count += 1) {
					html = target.innerHTML;
					if (html.indexOf("http://") !== -1 || html.indexOf("https://") !== -1) return html;
					target = target.parentNode;
					count += 1;
				}
			} catch {}
		}
		const linksSrc = [];
		if (window.parent !== window) {
			if (window.document && window.document.scripts) for (let i = 0; i < window.document.scripts.length; i += 1) {
				const src = window.document.scripts[i].src;
				if (src.startsWith("http")) linksSrc.push(src);
			}
		}
		let node = ev.target;
		if (node.nodeType !== 1) node = node.parentNode;
		let href = null;
		if (node.closest("a[href]")) href = node.closest("a[href]").getAttribute("href");
		return {
			event: { target: {
				baseURI: ev.target.baseURI,
				value: ev.target.value,
				href: ev.target.href,
				parentNode: { href: ev.target.parentNode ? ev.target.parentNode.href : null },
				linksSrc
			} },
			context: getContextHTML(ev),
			href
		};
	}
	//#endregion
	//#region src/content_scripts/whotracksme.js
	window.addEventListener("mousedown", (ev) => {
		const { event, context, href } = recordMouseDown(ev);
		chrome.runtime.sendMessage({
			action: "mousedown",
			event,
			context,
			href
		});
	});
	//#endregion
})();
