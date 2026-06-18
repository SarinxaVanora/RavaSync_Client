import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import ManagedConfig from "../../../store/managed-config.js";
import Options, { getPausedDetails } from "../../../store/options.js";
import { categories, regions } from "../../../ui/labels.js";
import { showCopyNotification } from "../components/alert.js";
import { openHref } from "../../../utils/tabs.js";
import { getLabel, getStatus } from "../../../utils/exceptions.js";
import TabStats from "../../../store/tab-stats.js";
import protection_status_default from "./protection-status.js";
//#region src/pages/panel/views/tracker-details.js
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
function cleanUp(text) {
	return text.replace(/(\\"|\\n|\\t|\\r)/g, "").trim();
}
var tracker_details_default = {
	[router_default.connect]: { dialog: true },
	options: store_default(Options),
	stats: store_default(TabStats),
	managedConfig: store_default(ManagedConfig),
	trackerId: "",
	tracker: ({ stats, trackerId }) => stats.trackers.find((t) => t.id === trackerId),
	exceptionStatus: ({ options, stats, tracker }) => getStatus(options, tracker.id, stats.hostname),
	exceptionLabel: ({ options, stats, tracker }) => getLabel(options, tracker.id, stats.hostname),
	wtmUrl: ({ tracker }) => tracker.category !== "unidentified" && `https://www.ghostery.com/whotracksme/trackers/${tracker.id}`,
	paused: ({ options, stats }) => store_default.ready(options, stats) && !!getPausedDetails(options, stats.hostname),
	render: ({ options, managedConfig, tracker, exceptionStatus, exceptionLabel, wtmUrl, paused }) => html`
    <template layout="column">
      <panel-dialog>
        <div id="panel-company-alerts" layout="absolute inset:1 bottom:auto"></div>
        <ui-text slot="header" type="label-l">${tracker.name}</ui-text>

        <div slot="header" layout="center row items:center gap overflow margin:0.5:0:0:0">
          <ui-category-icon name="${tracker.category}" layout="size:2.5"></ui-category-icon>
          <ui-text slot="header" type="body-s" color="secondary">
            ${tracker.company && tracker.company !== tracker.name && tracker.company + " •"}
            ${categories[tracker.category]}
          </ui-text>
        </div>
        ${options.terms && !managedConfig.disableUserControl && html`
          <div layout="grid:1|max gap:0.5 padding:1.5 margin:-1.5:-1.5:-2 ::background:secondary">
            ${paused ? html`
                  <ui-button layout="width:full height:auto:6" disabled>
                    <div layout="row gap">
                      <ui-icon name="pause" color="inherit"></ui-icon>
                      <ui-text type="label-m" color="inherit"> Ghostery paused </ui-text>
                    </div>
                  </ui-button>
                ` : html`<ui-button layout="width:full height:auto:6">
                  <a
                    href="${router_default.url(protection_status_default, { trackerId: tracker.id })}"
                    layout="row gap padding:0:1.5"
                  >
                    <ui-icon
                      name="${exceptionStatus.trusted ? "trust" : "block"}-m"
                      color="secondary"
                      layout="size:2"
                    ></ui-icon>
                    <ui-text type="label-m" layout="block:center row gap center padding:2px:0">
                      ${exceptionLabel}
                    </ui-text>
                  </a>
                </ui-button>`}
            ${tracker.category !== "unidentified" && html`
              <ui-button layout="width:6 height:auto:6">
                <a
                  href="${chrome.runtime.getURL(`/pages/settings/index.html#@settings-tracker-details?tracker=${tracker.id}`)}"
                  onclick="${openHref}"
                >
                  <ui-icon name="settings-m" layout="size:2"></ui-icon>
                </a>
              </ui-button>
            `}
          </div>
          <ui-line layout="margin:0:-1.5:0:-1.5"></ui-line>
        `}
        ${(store_default.ready(tracker.organization) || wtmUrl) && html`
          <div layout="column gap:0.5">
            ${store_default.ready(tracker.organization) && tracker.organization.description && html`<ui-text type="body-s">${cleanUp(tracker.organization?.description)}</ui-text>`}
            ${wtmUrl && html`
              <ui-text type="label-xs" color="brand-primary" underline>
                <a href="${wtmUrl}" onclick="${openHref}">Read more on WhoTracks.Me</a>
              </ui-text>
            `}
          </div>
          <ui-line></ui-line>
        `}
        <section
          layout="
            grid:max|1 items:start:stretch content:start gap:1:2.5
            grow:1
          "
        >
          ${tracker.requestsBlocked.length > 0 && html`
            <ui-icon name="block-s" color="danger-primary"></ui-icon>
            <div layout="column gap">
              <ui-text type="label-s">URLs blocked</ui-text>
              ${tracker.requestsBlocked.map(({ url }) => html`
                  <panel-copy oncopy="${showCopyNotification}">${url}</panel-copy>
                `)}
            </div>
          `}
          ${tracker.requestsModified.length > 0 && html`
            <ui-icon name="eye" color="brand-primary"></ui-icon>
            <div layout="column gap">
              <ui-text type="label-s">URLs modified</ui-text>
              ${tracker.requestsModified.map(({ url }) => html`
                  <panel-copy oncopy="${showCopyNotification}">${url}</panel-copy>
                `)}
            </div>
          `}
          ${tracker.requestsObserved.length > 0 && html`
            <ui-icon name="shield"></ui-icon>
            <div layout="column gap">
              <ui-text type="label-s">URLs observed</ui-text>
              ${tracker.requestsObserved.map(({ url }) => html`
                  <panel-copy oncopy="${showCopyNotification}">${url}</panel-copy>
                `)}
            </div>
          `}
          ${store_default.ready(tracker.organization) && tracker.organization.country && html`
            <ui-icon name="pin"></ui-icon>
            <div layout="column gap">
              <ui-text type="label-s">Country</ui-text>
              <ui-text type="body-s" color="secondary" ellipsis layout="padding margin:-1">
                ${regions.of(tracker.organization.country) || tracker.organization.country}
              </ui-text>
            </div>
          `}
          ${tracker.websiteUrl && html`
            <ui-icon name="globe"></ui-icon>
            <div layout="column gap">
              <ui-text type="label-s">Website</ui-text>
              <ui-text
                type="body-s"
                color="brand-primary"
                ellipsis
                underline
                layout="padding margin:-1"
              >
                <a href="${tracker.websiteUrl}" onclick="${openHref}"> ${tracker.websiteUrl} </a>
              </ui-text>
            </div>
          `}
          ${store_default.ready(tracker.organization) && html`
            ${tracker.organization.websiteUrl && html`
              <ui-icon name="globe"></ui-icon>
              <div layout="column gap">
                <ui-text type="label-s">Organization's website</ui-text>
                <ui-text
                  type="body-s"
                  color="brand-primary"
                  ellipsis
                  underline
                  layout="padding margin:-1"
                >
                  <a href="${tracker.organization.websiteUrl}" onclick="${openHref}">
                    ${tracker.organization.websiteUrl}
                  </a>
                </ui-text>
              </div>
            `}
            ${tracker.organization.privacyPolicy && html`
              <ui-icon name="privacy"></ui-icon>
              <div layout="column gap">
                <ui-text type="label-s">Privacy policy</ui-text>
                <ui-text
                  type="body-s"
                  color="brand-primary"
                  ellipsis
                  underline
                  layout="padding margin:-1"
                >
                  <a href="${tracker.organization.privacyPolicy}" onclick="${openHref}">
                    ${tracker.organization.privacyPolicy}
                  </a>
                </ui-text>
              </div>
            `}
            ${tracker.organization.contact && html`
              <ui-icon name="mail"></ui-icon>
              <div layout="column gap">
                <ui-text type="label-s">Contact</ui-text>
                <ui-text
                  type="body-s"
                  color="brand-primary"
                  ellipsis
                  underline
                  layout="padding margin:-1"
                >
                  <a
                    href="${tracker.organization.contact.startsWith("http") ? "" : "mailto:"}${tracker.organization.contact}"
                    onclick="${openHref}"
                  >
                    ${tracker.organization.contact}
                  </a>
                </ui-text>
              </div>
            `}
          `}
        </section>
      </panel-dialog>
    </template>
  `
};
//#endregion
export { tracker_details_default as default };
