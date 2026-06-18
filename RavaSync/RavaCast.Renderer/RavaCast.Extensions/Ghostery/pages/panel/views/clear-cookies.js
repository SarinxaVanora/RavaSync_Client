import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import { showAlert } from "../components/alert.js";
import TabStats from "../../../store/tab-stats.js";
//#region src/pages/panel/views/clear-cookies.js
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
async function clearCookies(host) {
	const result = await chrome.runtime.sendMessage({
		action: "cookies:clean",
		domain: host.stats.domain
	});
	showAlert(html`<panel-alert type="${result.success ? "success" : "danger"}" autoclose="2">
      ${result.success ? msg`Cookies successfully cleared` : msg`Failed to clear cookies`}
    </panel-alert>`);
}
var clear_cookies_default = {
	[router_default.connect]: { dialog: true },
	stats: store_default(TabStats),
	render: ({ stats }) => html`
    <template layout="column">
      <panel-dialog>
        <div layout="block:center column gap:2 padding:2:0">
          <ui-text type="label-l">Clear cookies?</ui-text>
          <ui-text type="body-s" color="tertiary">
            You’re about to remove all cookies stored by ${stats.domain}.
          </ui-text>
          <ui-text type="body-s" color="tertiary">
            This will sign you out and may reset preferences or saved settings. Some pages may not
            work until you sign in or accept cookies again.
          </ui-text>
        </div>
        <div layout="grid:2 gap">
          <ui-button>
            <a href="${router_default.backUrl()}">Cancel</a>
          </ui-button>
          <ui-button type="danger" data-qa="button:confirm-clear-cookies">
            <a onclick="${clearCookies}" href="${router_default.backUrl()}"> Clear cookies </a>
          </ui-button>
        </div>
      </panel-dialog>
    </template>
  `
};
//#endregion
export { clear_cookies_default as default };
