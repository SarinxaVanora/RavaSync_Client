import store_default from "../npm/hybrids/src/store.js";
//#region src/store/notification.js
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
async function loadFromStorage() {
	const { notifications } = await chrome.storage.local.get("notifications");
	return notifications || {};
}
var Notification = {
	id: true,
	shown: 0,
	lastShownAt: 0,
	[store_default.connect]: {
		async get(id) {
			return (await loadFromStorage())[id];
		},
		async set(id, values) {
			const notifications = await loadFromStorage();
			notifications[values.id] = values;
			await chrome.storage.local.set({ notifications });
			return values;
		},
		async list() {
			return Object.values(await loadFromStorage());
		},
		loose: true
	}
};
chrome.storage.onChanged.addListener((changes) => {
	if (changes["notifications"]) store_default.clear(Notification, false);
});
//#endregion
export { Notification as default };
