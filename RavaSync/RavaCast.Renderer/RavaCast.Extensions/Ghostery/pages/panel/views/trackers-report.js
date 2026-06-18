import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { isWebkit } from "../../../utils/browser-info.js";
import Options from "../../../store/options.js";
import { numberFormatter } from "../../../ui/labels.js";
import { stringify } from "../../../npm/csv-stringify/dist/esm/sync.js";
import { download } from "../../../utils/files.js";
import { showCopyNotification } from "../components/alert.js";
import TabStats from "../../../store/tab-stats.js";
//#region src/pages/panel/views/trackers-report.js
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
var REPORT_COLUMNS = [
	"Tracker",
	"Organization",
	"URL"
];
async function downloadReport(host, event) {
	const button = event.currentTarget;
	button.disabled = true;
	const report = [];
	for (const tracker of host.trackers) {
		const organizationName = (tracker.organization && await store_default.resolve(tracker.organization))?.name ?? "";
		for (const request of tracker.requestsBlocked) report.push([
			tracker.name,
			organizationName,
			request.url
		]);
	}
	await download({
		data: stringify(report, {
			header: true,
			columns: REPORT_COLUMNS,
			escape_formulas: true
		}),
		filename: `ghostery-website-report-${host.stats.hostname}.csv`,
		type: "text/csv;charset=utf-8;",
		forceNewTab: isWebkit()
	});
	button.disabled = false;
}
var trackers_report_default = {
	[router_default.connect]: { dialog: true },
	type: "",
	options: store_default(Options),
	stats: store_default(TabStats),
	trackers: ({ type, stats }) => stats.trackers.filter((t) => t[type]),
	render: ({ type, stats, trackers }) => html`
    <template layout="column">
      <panel-dialog header>
        <div slot="header" layout="block:center column center">
          <div layout="row items:center gap:0.5">
            ${type === "blocked" && html`
              <panel-badge type="danger">${trackers.length}</panel-badge>
              <ui-text type="label-m">Trackers blocked</ui-text>
            `}
            ${type === "modified" && html`
              <panel-badge type="brand">${trackers.length}</panel-badge>
              <ui-text type="label-m">Trackers modified</ui-text>
            `}
          </div>
          <ui-text type="body-s" color="secondary">${stats.hostname}</ui-text>
        </div>
        <div layout="column gap:2.5">
          ${trackers.map((tracker) => html`
              <div layout="column gap">
                <div layout="row items:center gap:0.5">
                  <ui-text type="label-s">${tracker.name}</ui-text>
                  <ui-category-icon name="${tracker.category}" size="small"></ui-category-icon>
                  <ui-stats-badge layout="height:full">
                    ${type === "blocked" && numberFormatter.format(tracker.requestsBlocked.length)}
                    ${type === "modified" && numberFormatter.format(tracker.requestsModified.length)}
                  </ui-stats-badge>
                </div>
                ${type === "blocked" && tracker.requestsBlocked.map(({ url }) => html`
                    <panel-copy oncopy="${showCopyNotification}"> ${url} </panel-copy>
                  `)}
                ${type === "modified" && tracker.requestsModified.map(({ url }) => html`
                    <panel-copy oncopy="${showCopyNotification}"> ${url} </panel-copy>
                  `)}
              </div>
            `)}
          <ui-button type="primary" onclick="${downloadReport}" layout="shrink:0">
            <button><ui-icon name="download"></ui-icon> Download report</button>
          </ui-button>
        </div>
      </panel-dialog>
    </template>
  `
};
//#endregion
export { trackers_report_default as default };
