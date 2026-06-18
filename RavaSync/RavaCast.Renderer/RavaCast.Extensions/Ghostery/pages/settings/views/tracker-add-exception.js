import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import Options from "../../../store/options.js";
import Tracker from "../../../store/tracker.js";
import { toggleDomain } from "../../../utils/exceptions.js";
import hostname_default from "../store/hostname.js";
//#region src/pages/settings/views/tracker-add-exception.js
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
async function add({ options, tracker, hostname }, event) {
	event.preventDefault();
	router_default.resolve(event, store_default.submit(hostname).then(({ value }) => toggleDomain(options, tracker.id, value, true)));
}
var tracker_add_exception_default = {
	[router_default.connect]: { dialog: true },
	options: store_default(Options),
	tracker: store_default(Tracker),
	hostname: store_default(hostname_default, { draft: true }),
	render: ({ tracker, hostname }) => html`
    <template layout>
      ${store_default.ready(tracker) && html`
        <settings-dialog closable>
          <form action="${router_default.backUrl()}" onsubmit="${add}" layout="column gap:3">
            <ui-text type="label-l" layout="block:center margin:bottom">
              Add website exception
            </ui-text>
            <div layout="column gap:2">
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
              <ui-text type="body-s" color="secondary">
                ${tracker.name} will be trusted on this website.
              </ui-text>
            </div>
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
      `}
    </template>
  `
};
//#endregion
export { tracker_add_exception_default as default };
