import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import Options from "../../../store/options.js";
import hostname_default from "../store/hostname.js";
//#region src/pages/settings/views/redirect-protection-add-exception.js
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
async function add({ options, hostname }, event) {
	event.preventDefault();
	router_default.resolve(event, store_default.submit(hostname).then(({ value }) => {
		if (options.redirectProtection.exceptions[value]) return;
		return store_default.set(options, { redirectProtection: { exceptions: { [value]: true } } });
	}));
}
var redirect_protection_add_exception_default = {
	[router_default.connect]: { dialog: true },
	options: store_default(Options),
	hostname: store_default(hostname_default, { draft: true }),
	render: ({ hostname }) => html`
    <template layout>
      <settings-dialog closable>
        <form action="${router_default.backUrl()}" onsubmit="${add}" layout="column gap:3">
          <ui-text type="label-l" layout="block:center margin:bottom"> Add exception </ui-text>
          <div layout="column gap items:start">
            <ui-text type="body-m" color="secondary">
              Enter the website URL you want to allow redirects from. Ghostery will not show the
              warning page for this domain.
            </ui-text>
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
                data-qa="input:redirect-protection:hostname"
              />
            </ui-input>
          </div>
          <div layout="grid:1|1 gap margin:top:2">
            <ui-button>
              <a href="${router_default.backUrl()}" tabindex="2">Cancel</a>
            </ui-button>
            <ui-button type="primary" data-qa="button:redirect-protection:save">
              <button type="submit" tabindex="1">Save</button>
            </ui-button>
          </div>
        </form>
      </settings-dialog>
    </template>
  `
};
//#endregion
export { redirect_protection_add_exception_default as default };
