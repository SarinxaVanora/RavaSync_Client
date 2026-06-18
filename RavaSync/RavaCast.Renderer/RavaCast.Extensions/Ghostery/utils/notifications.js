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
var MOUNT_ACTION = "notifications:mount";
var UNMOUNT_ACTION = "notifications:unmount";
var RESIZE_WINDOW_EVENT = "ghostery:notifications:resize";
var CLOSE_WINDOW_EVENT = "ghostery:notifications:close";
function setupNotificationPage(width = 440) {
	new ResizeObserver(() => {
		window.parent.postMessage({
			type: RESIZE_WINDOW_EVENT,
			height: document.body.clientHeight,
			width
		}, "*");
	}).observe(document.body, { box: "border-box" });
	document.body.style.overflow = "hidden";
	chrome.runtime.onMessage.addListener((msg) => {
		if (msg.action === "notifications:clear" && location.pathname.split("/").pop() === msg.id) window.parent.postMessage({ type: CLOSE_WINDOW_EVENT }, "*");
	});
	return ({ clear = true, reload = false } = {}) => {
		setTimeout(() => {
			window.parent.postMessage({
				type: CLOSE_WINDOW_EVENT,
				clear,
				reload
			}, "*");
		}, 100);
	};
}
//#endregion
export { CLOSE_ACTION, MOUNT_ACTION, OPEN_ACTION, UNMOUNT_ACTION, setupNotificationPage };
