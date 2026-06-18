import store_default from "../npm/hybrids/src/store.js";
import { getOS, isOpera, isWebkit } from "../utils/browser-info.js";
import ManagedConfig from "../store/managed-config.js";
import Notification from "../store/notification.js";
import Options from "../store/options.js";
import { checkStorage } from "../utils/storage.js";
import "../npm/@ghostery/config/dist/esm/flags.js";
import Config from "../store/config.js";
import { isSerpSupported } from "../utils/opera.js";
import { getStorage } from "../utils/telemetry.js";
import { CLOSE_ACTION, MOUNT_ACTION, OPEN_ACTION, UNMOUNT_ACTION } from "../utils/notifications.js";
import "./onboarding.js";
//#region src/background/notifications.js
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
async function openNotification({ id, tabId, shownLimit = 0, delay, params, position }) {
	const options = await store_default.resolve(Options);
	const managedConfig = await store_default.resolve(ManagedConfig);
	const notification = await store_default.resolve(Notification, id).catch(() => null);
	if (!options.terms || managedConfig.disableNotifications || shownLimit > 0 && notification?.shown >= shownLimit || delay && notification?.lastShownAt && Date.now() - notification.lastShownAt < delay) return false;
	const url = chrome.runtime.getURL(`/pages/notifications/${id}.html`) + (params ? `?${Object.entries(params).map(([key, value]) => `${key}=${encodeURIComponent(value)}`).join("&")}` : "");
	try {
		await checkStorage();
		const mounted = await chrome.tabs.sendMessage(tabId, {
			action: MOUNT_ACTION,
			url,
			position,
			debug: false
		});
		if (mounted) {
			await store_default.set(Notification, {
				id,
				shown: (notification?.shown || 0) + 1,
				lastShownAt: Date.now()
			});
			console.log(`[notifications] Opened notification "${id}" with params:`, params);
		}
		return mounted;
	} catch (e) {
		console.error(`[notifications] Failed to open notification "${id}" in tab:`, tabId, e);
		return false;
	}
}
function closeNotification(tabId) {
	return chrome.tabs.sendMessage(tabId, { action: UNMOUNT_ACTION });
}
chrome.runtime.onMessage.addListener((msg, sender) => {
	const tabId = sender.tab?.id;
	if (!tabId) return;
	switch (msg.action) {
		case OPEN_ACTION:
			openNotification({
				tabId,
				...msg
			});
			break;
		case CLOSE_ACTION:
			closeNotification(tabId);
			break;
	}
});
if (!isWebkit() && getOS() !== "android") chrome.webNavigation.onCompleted.addListener(async (details) => {
	if (!details.url.startsWith("http") || details.frameId !== 0 || (await chrome.action.getUserSettings()).isOnToolbar) return;
	if (details.url === "https://blocksurvey.io/install-survey-postonboarding-R6q0d5dGR9OY6202iNPmGQ?v=o") return;
	openNotification({
		id: "pin-it",
		tabId: details.tabId,
		shownLimit: 1,
		position: "center"
	});
});
var REVIEW_NOTIFICATION_DELAY = 720 * 60 * 60 * 1e3;
chrome.webNavigation.onCompleted.addListener(async (details) => {
	if (!details.url.startsWith("http") || details.frameId !== 0) return;
	const { installDate } = await getStorage();
	if (!installDate) return;
	if (!(await store_default.resolve(Config)).hasFlag("notification-review")) return;
	if (Date.now() - new Date(installDate).getTime() >= REVIEW_NOTIFICATION_DELAY) openNotification({
		id: "review",
		tabId: details.tabId,
		shownLimit: 1,
		position: "center"
	});
});
if (isOpera()) {
	const NOTIFICATION_DELAY = 10080 * 60 * 1e3;
	const NOTIFICATION_SHOW_LIMIT = 4;
	chrome.webNavigation.onCompleted.addListener(async (details) => {
		if (!details.url.startsWith("http") || details.frameId !== 0 || await isSerpSupported()) return;
		openNotification({
			id: "opera-serp",
			tabId: details.tabId,
			shownLimit: NOTIFICATION_SHOW_LIMIT,
			delay: NOTIFICATION_DELAY,
			position: "center"
		});
	});
}
//#endregion
export { openNotification };
