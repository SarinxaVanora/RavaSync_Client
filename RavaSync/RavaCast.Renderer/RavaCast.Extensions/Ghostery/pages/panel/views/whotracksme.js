import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { parse } from "../../../npm/tldts-experimental/dist/es6/index.js";
import { WTM_PAGE_URL } from "../../../utils/urls.js";
import { openHref } from "../../../utils/tabs.js";
import TabStats from "../../../store/tab-stats.js";
//#region src/pages/panel/views/whotracksme.js
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
function hasWTMStats(domain) {
	return chrome.runtime.sendMessage({
		action: "hasWTMStats",
		domain
	});
}
var whotracksme_default = {
	[router_default.connect]: { dialog: true },
	stats: store_default(TabStats),
	domain: ({ stats }) => store_default.ready(stats) && parse(stats.hostname).domain,
	wtmLink: async ({ domain }) => {
		if (!domain) return null;
		return await hasWTMStats(domain) && `${WTM_PAGE_URL}/websites/${domain}`;
	},
	render: ({ domain, wtmLink }) => html`
    <template layout="column">
      <panel-dialog>
        <ui-text slot="header" type="label-m" layout="padding:1:0"> WhoTracks.Me Reports </ui-text>
        <div layout="column gap:2 padding:1:0">
          <ui-action>
            <a
              href="${chrome.runtime.getURL("/pages/whotracksme/index.html")}"
              onclick="${openHref}"
              layout="row gap:2"
            >
              <ui-icon name="whotracksme" color="tertiary" layout="size:3"></ui-icon>
              <div layout="column gap:0.5">
                <ui-text type="label-m">Your Browser Privacy Report</ui-text>
                <ui-text type="body-s" color="tertiary">
                  Generates a global transparency report on web tracking in your Ghostery-protected
                  browser.
                </ui-text>
              </div>
              <ui-icon name="chevron-right-s" color="quaternary"></ui-icon>
            </a>
          </ui-action>
          ${html.resolve(wtmLink.then((link) => link && html`
                  <ui-action>
                    <a href="${link}" onclick="${openHref}" layout="row gap:2">
                      <ui-icon name="stats-report" color="tertiary" layout="size:3"></ui-icon>
                      <div layout="column gap:0.5">
                        <ui-text type="label-m"> Website Statistical Report </ui-text>
                        <ui-text type="body-s" color="tertiary">
                          Displays unique insights into observed activities on ${domain}, revealing
                          affiliation, categories, and presence across the site.
                        </ui-text>
                      </div>
                      <ui-icon name="external-link" color="quaternary" layout="size:2"></ui-icon>
                    </a>
                  </ui-action>
                `))}
        </div>
      </panel-dialog>
    </template>
  `
};
//#endregion
export { whotracksme_default as default };
