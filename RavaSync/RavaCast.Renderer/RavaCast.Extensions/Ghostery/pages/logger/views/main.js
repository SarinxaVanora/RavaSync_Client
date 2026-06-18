import store_default from "../../../npm/hybrids/src/store.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { FilterType } from "../../../npm/@ghostery/adblocker/dist/esm/lists.js";
import "../../../npm/@ghostery/adblocker/dist/esm/index.js";
import log_default from "../store/log.js";
import { stringify } from "../../../npm/csv-stringify/dist/esm/sync.js";
import { download } from "../../../utils/files.js";
import tab_default from "../store/tab.js";
import "../assets/refresh.js";
//#region src/pages/logger/views/main.js
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
function refreshSelectedTab(host) {
	chrome.tabs.reload(Number(host.tabId));
}
var REPORT_COLUMNS = [
	"Date",
	"Filter",
	"Type",
	"Blocked",
	"Modified",
	"URL",
	"Tracker",
	"Organization"
];
async function downloadReport(host) {
	await download({
		data: stringify(host.logs.map((log) => [
			new Date(log.timestamp).toISOString(),
			log.typeLabel,
			log.filter,
			log.blocked,
			log.modified,
			log.url,
			log.tracker,
			log.organization ?? ""
		]), {
			header: true,
			columns: REPORT_COLUMNS,
			escape_formulas: true
		}),
		filename: `ghostery-logger-report.csv`,
		type: "text/csv;charset=utf-8;"
	});
}
function disableEllipsis(host, event) {
	const textElements = event.currentTarget.querySelectorAll("ui-text");
	for (const el of textElements) el.ellipsis = false;
}
var main_default = {
	logs: store_default([log_default], { id: ({ tabId, query, filterType }) => ({
		tabId,
		query,
		filterType
	}) }),
	tabs: store_default([tab_default]),
	tabId: ({ tabs }, value) => {
		if (value !== void 0) return value;
		return store_default.ready(tabs) ? tabs.find((tab) => tab.active).id : "";
	},
	query: "",
	filterType: 0,
	render: ({ logs, tabs, tabId, filterType }) => html`
    <template layout="height:full width::960px">
      <main layout="column height:full" translate="no">
        <div layout="row items:center gap padding:2:2:1">
          <ui-text type="label-l">Logger</ui-text>
          <ui-input layout="width:30">
            <select onchange="${html.set("tabId")}" value="${tabId}">
              <option value="">All tabs</option>
              ${store_default.ready(tabs) && tabs.map((tab) => html`
                  <option value="${tab.id}" selected=${tab.id === tabId}>
                    ${tab.title} ${tab.hostname}
                  </option>
                `)}
            </select>
          </ui-input>
          <ui-input layout="grow">
            <input type="search" placeholder="Search..." oninput="${html.set("query")}" />
          </ui-input>
          <ui-input>
            <select onchange="${html.set("filterType")}" value="${filterType}">
              <option value="0">All filters</option>
              <option value="${FilterType.NETWORK}">Network</option>
              <option value="${FilterType.COSMETIC}">Cosmetic</option>
            </select>
          </ui-input>
          <ui-button
            onclick="${downloadReport}"
            layout="width:5"
            disabled="${!store_default.ready(logs) || logs.length === 0}"
          >
            <button title="Download report">
              <ui-icon name="download" layout="size:2"></ui-icon>
            </button>
          </ui-button>
          <ui-button onclick="${refreshSelectedTab}" layout="width:5" disabled="${!tabId}">
            <button title="Refresh current tab">
              <ui-icon name="refresh" layout="size:2"></ui-icon>
            </button>
          </ui-button>

          <ui-button onclick="${() => location.reload()}" layout="width:5">
            <button title="Clear logs and reload">
              <ui-icon name="trash" layout="size:2"></ui-icon>
            </button>
          </ui-button>
        </div>
        <ui-line></ui-line>
        <div layout="grid:80px|120px|1fr|40px|240px|140px gap padding:1:2">
          <ui-text type="body-s" color="tertiary">Date</ui-text>
          <ui-text type="body-s" color="tertiary">Type</ui-text>
          <ui-text type="body-s" color="tertiary">Filter</ui-text>
          <ui-text type="body-s" color="tertiary"></ui-text>
          <ui-text type="body-s" color="tertiary">URL</ui-text>
          <ui-text type="body-s" color="tertiary">Tracker</ui-text>
        </div>
        <ui-line></ui-line>
        <div layout="column overflow:scroll grow">
          ${store_default.ready(logs) && logs.length === 0 && html`
            <div
              layout="block:center column grow center self:center gap width:::350px"
              translate="no"
            >
              <img src="${"/assets/refresh-B0nlARG8.svg"}" layout="size:221px" />
              <ui-text type="headline-s">
                To view the request log, press refresh on the toolbar
              </ui-text>
              <ui-text type="body-s" color="secondary">
                This is an experimental feature - if you encounter any issues, please contact our
                support team.
              </ui-text>
            </div>
          `}
          <div layout="column-reverse padding" style="word-break:break-all">
            ${store_default.ready(logs) && logs.map((log) => html`
                <div
                  layout="grid:80px|120px|1fr|40px|240px|140px gap padding:0.5:1"
                  layout:hover="::background:secondary"
                  onclick="${disableEllipsis}"
                >
                  <ui-text type="body-s" color="tertiary"> ${log.time} </ui-text>
                  <ui-text type="body-s" color="secondary"> ${log.typeLabel} </ui-text>
                  <ui-text ellipsis>${log.filter}</ui-text>
                  <div layout="row gap:0.5">
                    ${log.blocked && html`<ui-icon name="block-s" color="danger-primary" layout="size:2"></ui-icon>`}
                    ${log.modified && html`<ui-icon name="eye" color="brand-primary" layout="size:2"></ui-icon>`}
                  </div>
                  <ui-text type="body-s" color="tertiary" ellipsis> ${log.url} </ui-text>
                  <ui-text type="body-s" color="tertiary" ellipsis>
                    ${log.tracker} ${log.organization && html`(${log.organization})`}
                  </ui-text>
                </div>
              `.key(log.id))}
          </div>
        </div>
      </main>
    </template>
  `
};
//#endregion
export { main_default as default };
