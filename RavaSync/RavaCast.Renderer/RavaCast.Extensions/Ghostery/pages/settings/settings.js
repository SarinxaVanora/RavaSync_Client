import router_default from "../../npm/hybrids/src/router.js";
import { html } from "../../npm/hybrids/src/template/index.js";
import { BECOME_A_CONTRIBUTOR_PAGE_URL } from "../../utils/urls.js";
import assets_default from "./assets/index.js";
import privacy_default from "./views/privacy.js";
import whotracksme_default from "./views/whotracksme.js";
import websites_default from "./views/websites.js";
import my_ghostery_default from "./views/my-ghostery.js";
import trackers_default from "./views/trackers.js";
//#region src/pages/settings/settings.js
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
var settings_default = {
	stack: router_default([
		privacy_default,
		websites_default,
		whotracksme_default,
		my_ghostery_default,
		trackers_default
	], { transition: true }),
	render: ({ stack }) => html`
      <template layout="contents">
        <settings-layout data-qa="page:settings">
          <a
            href="${router_default.url(privacy_default)}"
            class="${{ active: router_default.active(privacy_default, { stack: true }) }}"
            slot="nav"
            data-qa="button:privacy-protection"
          >
            <ui-icon name="shield-menu" color="inherit" layout="size:3"></ui-icon>
            Privacy protection
          </a>
          <a
            href="${router_default.url(websites_default)}"
            class="${{ active: router_default.active(websites_default, { stack: true }) && !router_default.active(trackers_default, { stack: true }) }}"
            slot="nav"
            data-qa="button:websites"
          >
            <ui-icon name="websites" color="inherit" layout="size:3"></ui-icon>
            Websites
          </a>
          <a
            href="${router_default.url(trackers_default)}"
            class="${{ active: router_default.active(trackers_default, { stack: true }) }}"
            slot="nav"
            data-qa="button:trackers"
          >
            <ui-icon name="block-m" color="inherit" layout="size:3"></ui-icon>
            Trackers
          </a>
          <a
            href="${router_default.url(whotracksme_default)}"
            class="${{
		active: router_default.active(whotracksme_default),
		wrap: true
	}}"
            slot="nav"
            translate="no"
            data-qa="button:whotracksme"
          >
            <ui-icon name="wtm" color="inherit" layout="size:3"></ui-icon>
            WhoTracks.Me
          </a>

          <a
            href="${router_default.url(my_ghostery_default)}"
            class="${{
		active: router_default.active(my_ghostery_default),
		bottom: true
	}}"
            slot="nav"
            data-qa="button:my-ghostery"
          >
            <ui-icon name="user" color="inherit" layout="size:3"></ui-icon>
            My Ghostery
          </a>
          <settings-card
            static
            layout="hidden"
            layout@992px="block area::6/7 self:end:stretch margin:top:2 padding:2"
            slot="nav"
          >
            <div layout="column items:center gap">
              <img src="${assets_default.hands}" layout="size:12" alt="Contribution" slot="picture" />
              <div layout="column gap:0.5">
                <ui-text type="label-l" layout="block:center"> Become a Contributor </ui-text>
                <ui-text type="body-s" color="secondary" layout="block:center">
                  Help Ghostery fight for a web where privacy is a basic human right.
                </ui-text>
                <ui-button type="primary" layout="margin:top">
                  <a
                    href="${BECOME_A_CONTRIBUTOR_PAGE_URL}?utm_source=gbe&utm_campaign=settings-becomeacontributor"
                    target="_blank"
                  >
                    Become a Contributor
                  </a>
                </ui-button>
              </div>
            </div>
          </settings-card>
          <div layout="column grow height::0 view:main layer">${stack}</div>
        </settings-layout>
      </template>
    `.use(html.transition)
};
//#endregion
export { settings_default as default };
