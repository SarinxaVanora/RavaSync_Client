import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import Options, { GLOBAL_PAUSE_ID } from "../../../store/options.js";
import { longDateFormatter } from "../../../ui/labels.js";
import { BECOME_A_CONTRIBUTOR_PAGE_URL } from "../../../utils/urls.js";
import assets_default from "../assets/index.js";
import { asyncAction } from "../utils/actions.js";
import autoconsent_default from "./autoconsent.js";
import additional_filters_default, { getAdditionalFiltersLabel } from "./additional-filters.js";
import redirect_protection_default, { getRedirectProtectionLabel } from "./redirect-protection.js";
import distractions_default, { getDistractionsLabel } from "./distractions.js";
//#region src/pages/settings/views/privacy.js
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
function toggleNeverConsent({ options }) {
	store_default.set(options, { blockAnnoyances: !options.blockAnnoyances });
}
function updateGlobalPause({ options }, value, lastValue) {
	if (lastValue === void 0) return;
	store_default.set(options, { paused: { [GLOBAL_PAUSE_ID]: value ? { revokeAt: Date.now() + 1440 * 60 * 1e3 } : null } });
}
function updateEngines(host, event) {
	asyncAction(event, chrome.runtime.sendMessage({ action: "updateEngines" }));
}
var privacy_default = {
	[router_default.connect]: { stack: [
		autoconsent_default,
		additional_filters_default,
		redirect_protection_default,
		distractions_default
	] },
	options: store_default(Options),
	devMode: false,
	globalPause: {
		value: false,
		observe: updateGlobalPause
	},
	globalPauseRevokeAt: {
		value: ({ options }) => store_default.ready(options) && options.paused["<all_urls>"]?.revokeAt,
		observe: (host, value) => {
			host.globalPause = value;
		}
	},
	render: ({ options, devMode, globalPause, globalPauseRevokeAt }) => html`
    <template layout="contents">
      <settings-page-layout layout="column gap:4">
        ${store_default.ready(options) && html`
          <section layout="column gap:4">
            <div layout="column gap" layout@992px="margin:bottom">
              <ui-text type="headline-m">Privacy protection</ui-text>
              <ui-text type="body-l" mobile-type="body-m" color="secondary">
                Ghostery protects your privacy by detecting and neutralizing different types of data
                collection including ads, trackers, and cookie pop-ups.
              </ui-text>
            </div>
            <settings-toggle
              icon="pause"
              value="${globalPause}"
              onchange="${html.set("globalPause")}"
              data-qa="toggle:global-pause"
              layout@768px="margin:bottom:-3"
            >
              Pause Ghostery
              <span slot="description">Suspends privacy protection globally for 1 day.</span>
              ${globalPauseRevokeAt && html`
                <ui-text type="body-s" color="secondary" slot="footer">
                  <ui-revoke-at revokeAt="${globalPauseRevokeAt}"></ui-revoke-at>
                </ui-text>
              `}
            </settings-toggle>
            <div
              layout="column gap:5"
              style="${{ opacity: globalPause ? .5 : void 0 }}"
              inert="${globalPause}"
            >
              <div layout="column gap" layout@768px="grid:3">
                <settings-toggle
                  icon="block-ads"
                  value="${options.blockAds}"
                  onchange="${html.set(options, "blockAds")}"
                  data-qa="toggle:ad-blocking"
                >
                  Ad-Blocking
                  <span slot="description">
                    Eliminates ads on websites for safe and fast browsing.
                  </span>
                </settings-toggle>
                <settings-toggle
                  icon="anti-tracking"
                  value="${options.blockTrackers}"
                  onchange="${html.set(options, "blockTrackers")}"
                  data-qa="toggle:anti-tracking"
                >
                  Anti-Tracking
                  <span slot="description">
                    Prevents various tracking techniques using AI-driven technology.
                  </span>
                </settings-toggle>
                <settings-toggle
                  icon="never-consent"
                  value="${options.blockAnnoyances}"
                  onchange="${toggleNeverConsent}"
                  data-qa="toggle:never-consent"
                >
                  Never-Consent
                  <span slot="description">Automatically rejects cookie consent notices.</span>

                  <ui-text
                    type="label-m"
                    color="primary"
                    slot="footer"
                    inert="${!options.blockAnnoyances}"
                    style="${{ opacity: !options.blockAnnoyances ? .5 : void 0 }}"
                    layout="margin:top"
                  >
                    <a
                      href="${options.blockAnnoyances ? router_default.url(autoconsent_default) : ""}"
                      layout="row items:center gap:2px margin:-1 padding:1"
                    >
                      Extended settings
                      <ui-icon name="chevron-right-s"></ui-icon>
                    </a>
                  </ui-text>
                </settings-toggle>
              </div>
              <div layout="column gap">
                ${options.mode !== "zap" && html`
                  <settings-link
                    href="${router_default.url(redirect_protection_default)}"
                    icon="redirect-protection"
                    data-qa="button:redirect-protection"
                  >
                    Redirect Protection
                    <ui-text slot="footer" color="tertiary">
                      ${getRedirectProtectionLabel(options)}
                    </ui-text>
                  </settings-link>
                  <settings-link
                    href="${router_default.url(distractions_default)}"
                    icon="annoyances"
                    data-qa="button:distractions"
                  >
                    Distractions
                    <ui-text slot="footer" color="tertiary">
                      ${getDistractionsLabel(options)}
                    </ui-text>
                  </settings-link>
                `}

                <settings-link
                  href="${router_default.url(additional_filters_default)}"
                  data-qa="button:additional-filters"
                  icon="detailed-view"
                >
                  Additional Filters
                  <ui-text slot="footer" color="tertiary">
                    ${getAdditionalFiltersLabel(options)}
                  </ui-text>
                </settings-link>
              </div>
            </div>
          </section>

          <div>
            <settings-devtools
              onshown="${html.set("devMode", true)}"
              visible="${devMode}"
            ></settings-devtools>
            <div layout="row gap items:center">
              <ui-text type="body-m" color="tertiary">
                Last update:
                ${options.filtersUpdatedAt ? longDateFormatter.format(new Date(options.filtersUpdatedAt)) : html`updating...`}
              </ui-text>
              <ui-button
                type="outline"
                size="s"
                style="height:26px"
                onclick="${updateEngines}"
                disabled="${!options.filtersUpdatedAt}"
              >
                <button layout="padding:0:1">Update Now</button>
              </ui-button>
            </div>
          </div>
        `}
        <section layout="grid:1/1 grow items:end:stretch padding:0" layout@992px="hidden">
          <settings-card static>
            <div layout="column items:center gap padding:2" layout@768px="row gap:5">
              <img src="${assets_default["hands"]}" layout="size:12" alt="Contribution" />
              <div
                layout="block:center column gap:0.5"
                layout@768px="block:left row grow gap:5 content:space-between"
              >
                <div layout="column gap:0.5">
                  <ui-text type="label-l" layout=""> Become a Contributor </ui-text>
                  <ui-text type="body-s" color="secondary">
                    Help Ghostery fight for a web where privacy is a basic human right.
                  </ui-text>
                </div>
                <ui-button type="primary" layout="grow margin:top">
                  <a
                    href="${BECOME_A_CONTRIBUTOR_PAGE_URL}?utm_source=gbe&utm_campaign=privacy-becomeacontributor"
                    target="_blank"
                  >
                    Become a Contributor
                  </a>
                </ui-button>
              </div>
            </div>
          </settings-card>
        </section>
      </settings-page-layout>
    </template>
  `
};
//#endregion
export { privacy_default as default };
