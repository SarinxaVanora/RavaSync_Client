import mount from "../../npm/hybrids/src/mount.js";
import router_default from "../../npm/hybrids/src/router.js";
import { html } from "../../npm/hybrids/src/template/index.js";
import { getBrowser, getOS } from "../../utils/browser-info.js";
import "../../ui/index.js";
import main_default from "./views/main.js";
import "./elements.js";
/* empty css           */
//#region src/pages/panel/index.js
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
mount(document.body, {
	stack: router_default([main_default]),
	browserName: {
		value: getBrowser().name,
		reflect: true
	},
	platformName: {
		value: getOS(),
		reflect: true
	},
	render: ({ stack }) => html`
    <template layout="row">
      <div id="alert-container" layout="fixed inset:1 top:1 bottom:auto layer:500"></div>
      ${stack}
    </template>
  `
});
chrome.runtime.sendMessage({
	action: "telemetry:ping",
	event: "engaged"
});
chrome.runtime.sendMessage({ action: "syncOptions" });
setInterval(() => chrome.runtime.sendMessage({ action: "keepAlive" }), 15e3);
//#endregion
