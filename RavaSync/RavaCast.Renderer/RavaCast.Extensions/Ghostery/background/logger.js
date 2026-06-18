import { FilterType } from "../npm/@ghostery/adblocker/dist/esm/lists.js";
import "../npm/@ghostery/adblocker/dist/esm/index.js";
import { MAIN_ENGINE, get } from "../utils/engines.js";
import { getMetadata, getOrganizations } from "../utils/trackerdb.js";
import { setup } from "./adblocker/engines.js";
import debounce from "../utils/debounce.js";
//#region src/background/logger.js
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
var ports = /* @__PURE__ */ new Set();
var logs = /* @__PURE__ */ new Map();
var sendData = debounce(function() {
	if (logs.size === 0 || ports.size === 0) return;
	for (const port of ports) port.postMessage({
		action: "logger:data",
		data: [...logs.values()]
	});
	logs.clear();
}, {
	waitFor: 100,
	maxWait: 1e3
});
function add(data) {
	logs.set(data.id, data);
	sendData();
}
async function logRequests(requests) {
	if (ports.size === 0) return;
	setup.pending && await setup.pending;
	const engine = get(MAIN_ENGINE);
	for (const request of requests) {
		const { filter } = engine.match(request);
		if (!filter && request.blocked) {
			const organizations = await getOrganizations();
			const metadata = getMetadata(request);
			add({
				...request,
				tracker: metadata?.name,
				organization: organizations.get(metadata?.organization)?.name
			});
		}
	}
}
chrome.runtime.onConnect.addListener(async (port) => {
	if (port.name === "logger") {
		if (ports.size === 0) {
			setup.pending && await setup.pending;
			const organizations = await getOrganizations();
			const engine = get(MAIN_ENGINE);
			const logFilterMatched = function({ filter }, { url, request, filterType, callerContext }) {
				if (filterType === FilterType.COSMETIC && filter.isScriptInject()) {
					filter = String(filter);
					const scriptInjectArgumentIndex = filter.indexOf("+js(") + 4;
					filter = filter.slice(0, scriptInjectArgumentIndex) + decodeURIComponent(filter.slice(scriptInjectArgumentIndex, -1)) + ")";
				} else filter = String(filter);
				let data = {
					filter,
					filterType,
					url,
					tabId: callerContext?.tabId
				};
				if (filterType === FilterType.COSMETIC) Object.assign(data, {
					id: `${url}-${filter}`,
					timestamp: Date.now()
				});
				else if (request) {
					const metadata = getMetadata(request);
					data = {
						...data,
						...request,
						tracker: metadata?.name,
						organization: organizations.get(metadata?.organization)?.name
					};
				}
				add(data);
			};
			engine.on("filter-matched", logFilterMatched);
			port.onDisconnect.addListener(() => {
				ports.delete(port);
				console.log("[logger] Disconnected logger with id", port.sender.tab.id);
				if (ports.size === 0) engine.unsubscribe("filter-matched", logFilterMatched);
			});
		}
		ports.add(port);
		console.log("[logger] Connected logger with id", port.sender.tab.id);
	}
});
//#endregion
export { logRequests };
