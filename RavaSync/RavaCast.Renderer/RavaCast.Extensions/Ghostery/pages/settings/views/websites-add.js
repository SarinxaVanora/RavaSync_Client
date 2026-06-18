import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import Options, { MODE_DEFAULT } from "../../../store/options.js";
import hostname_default from "../store/hostname.js";
//#region src/pages/settings/views/websites-add.js
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
async function add({ options, hostname, pauseType }, event) {
	event.preventDefault();
	router_default.resolve(event, store_default.submit(hostname).then(({ value }) => {
		switch (options.mode) {
			case MODE_DEFAULT:
				if (options.paused[value]) return;
				return store_default.set(options, { paused: { [value]: { revokeAt: pauseType && Date.now() + 3600 * 1e3 * pauseType } } });
			case "zap": return store_default.set(options, { zapped: { [value]: true } });
		}
	}));
}
var websites_add_default = {
	[router_default.connect]: { dialog: true },
	options: store_default(Options),
	hostname: store_default(hostname_default, { draft: true }),
	pauseType: 1,
	render: ({ options, hostname, pauseType }) => html`
    <template layout>
      <settings-dialog closable>
        <form action="${router_default.backUrl()}" onsubmit="${add}" layout="column gap:3">
          <ui-text type="label-l" layout="block:center margin:bottom"> Add website </ui-text>
          <div layout="column gap items:start">
            ${options.mode === "default" && html` <ui-text>To adjust privacy protection trust a site:</ui-text> `}
            ${options.mode === "zap" && html` <ui-text>To adjust privacy protection enable on a site:</ui-text> `}
          </div>
          <div layout="column gap:0.5">
            <ui-text type="label-m">Website</ui-text>
            <ui-input error="${store_default.error(hostname) || ""}">
              <input
                type="text"
                placeholder="${msg`Enter website URL`}"
                value="${hostname.value}"
                oninput="${html.set(hostname, "value")}"
                tabindex="1"
              />
            </ui-input>
          </div>
          ${options.mode === "default" && html`
            <div layout="column gap:0.5">
              <ui-text type="label-m">Select time frame</ui-text>
              <ui-input>
                <select
                  type="text"
                  placeholder="${msg`Enter website URL`}"
                  value="${pauseType}"
                  oninput="${html.set("pauseType")}"
                  tabindex="2"
                >
                  <option value="1">1 hour</option>
                  <option value="24">1 day</option>
                  <option value="0">Always</option>
                </select>
              </ui-input>
            </div>
          `}
          <div layout="grid:1|1 gap margin:top:2">
            <ui-button>
              <a href="${router_default.backUrl()}" tabindex="2">Cancel</a>
            </ui-button>
            <ui-button type="primary">
              <button type="submit" tabindex="1">Save</button>
            </ui-button>
          </div>
        </form>
      </settings-dialog>
    </template>
  `
};
//#endregion
export { websites_add_default as default };
