import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import ManagedConfig from "../../../store/managed-config.js";
import Options from "../../../store/options.js";
import { TERMS_AND_CONDITIONS_URL } from "../../../utils/urls.js";
import addon_health_default from "./addon-health.js";
import "../../../npm/@ghostery/config/dist/esm/flags.js";
import Config from "../../../store/config.js";
import web_trackers_default from "./web-trackers.js";
import performance_default from "./performance.js";
import privacy_default from "./privacy.js";
import skip_default from "./skip.js";
import success_default from "./success.js";
import modes_default from "./modes.js";
//#region src/pages/onboarding/views/main.js
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
function acceptTerms(host, event) {
	router_default.resolve(event, store_default.set(Options, {
		terms: true,
		feedback: host.feedback,
		filtersUpdatedAt: Date.now()
	}));
}
var main_default = {
	[router_default.connect]: { stack: () => [
		addon_health_default,
		web_trackers_default,
		performance_default,
		privacy_default,
		skip_default
	] },
	config: store_default(Config),
	managedConfig: store_default(ManagedConfig),
	feedback: true,
	mode: ({ config, managedConfig }) => store_default.ready(config, managedConfig) && config.hasFlag("modes") && !managedConfig.disableModes,
	render: ({ feedback, mode }) => html`
    <template layout="column gap:2 width:::375px">
      <ui-card layout="gap:2" layout@390px="gap:3">
        <section layout="block:center column gap" layout@390px="margin:2:0:1">
          <ui-text type="body-m">Welcome to Ghostery</ui-text>
          <ui-text type="display-m">Enable Ghostery to get started</ui-text>
        </section>
        <div layout="column gap:2">
          <ui-text type="display-2xs" layout="block:center">
            Your Community‑Powered Privacy Features:
          </ui-text>
          <div layout="grid:3 gap">
            <onboarding-feature icon="onboarding-adblocking"> Ad-Blocking </onboarding-feature>
            <onboarding-feature icon="onboarding-anti-tracking"> Anti-Tracking </onboarding-feature>
            <onboarding-feature icon="onboarding-never-consent"> Never-Consent </onboarding-feature>
          </div>
        </div>
        <div layout="column gap:2">
          ${false}
          ${html`
            <ui-text type="body-s" underline data-qa="text:description">
              ${msg.html`
                Information about <a href="${router_default.url(web_trackers_default)}">web trackers</a>,
                <a href="${router_default.url(addon_health_default)}">add-on's health</a>, and
                <a href="${router_default.url(performance_default)}">performance telemetry</a>
                will be shared in accordance with our <a href="${router_default.url(privacy_default)}" target="_blank" rel="noreferrer">Privacy Policy</a>,
                advancing privacy protection for the Ghostery community.
              `}
            </ui-text>
          `}
          <ui-text type="body-s">
            Ghostery uses this information to provide its community-powered privacy features,
            ensuring that personal information—such as passwords, browsing history, and page
            content—is never collected.
          </ui-text>
        </div>
        <div layout="column gap:2">
          <ui-button type="success" layout="height:5.5" data-qa="button:enable">
            <a href="${router_default.url(mode ? modes_default : success_default)}" onclick="${acceptTerms}">
              Enable Ghostery
            </a>
          </ui-button>
          <onboarding-error-card layout="margin:top">
            <ui-text type="body-s" color="danger-secondary" layout="block:center">
              With Ghostery disabled, only the basic functionality of naming trackers is available.
            </ui-text>
            <ui-button type="outline-danger" data-qa="button:skip">
              <a href="${router_default.url(skip_default)}">Keep Disabled</a>
            </ui-button>
          </onboarding-error-card>
        </div>
      </ui-card>
      <ui-button type="transparent" layout="self:center">
        <a href="${TERMS_AND_CONDITIONS_URL}" target="_blank"> Terms & Conditions </a>
      </ui-button>
    </template>
  `
};
//#endregion
export { main_default as default };
