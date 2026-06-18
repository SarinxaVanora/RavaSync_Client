import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import Options from "../../../store/options.js";
import { getStatus, toggleGlobal } from "../../../utils/exceptions.js";
import tracker_details_default from "./tracker-details.js";
import TrackerCategory from "../../../store/tracker-category.js";
//#region src/pages/settings/views/trackers.js
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
var PATTERNS_LIMIT = 50;
function loadMore(category) {
	return (host) => {
		host.limits = {
			...host.limits || {},
			[category]: (host.limits?.[category] || PATTERNS_LIMIT) + PATTERNS_LIMIT
		};
	};
}
var timeout;
function setLazyQuery(host, event) {
	const value = event.target.value || "";
	clearTimeout(timeout);
	if (value.length >= 2) timeout = setTimeout(() => {
		host.query = value;
		host.category = "_all";
	}, 50);
	else {
		host.category = "";
		host.query = "";
	}
}
function isActive(category, key) {
	return category === key || category === "_all";
}
function isTrusted(options, tracker) {
	const status = getStatus(options, tracker.id);
	return status.trusted && status.global;
}
function toggleException(tracker) {
	return async ({ options }) => toggleGlobal(options, tracker.id);
}
function clearCategory(id) {
	return async ({ options }) => {
		const exceptions = store_default.get(TrackerCategory, id).trackers.filter((t) => options.exceptions[t.id]).reduce((acc, t) => {
			acc[t.id] = null;
			return acc;
		}, {});
		await store_default.set(options, { exceptions });
	};
}
var trackers_default = {
	[router_default.connect]: { stack: () => [tracker_details_default] },
	options: store_default(Options),
	categories: ({ query, filter }) => store_default.get([TrackerCategory], {
		query,
		filter
	}),
	category: "",
	limits: void 0,
	query: "",
	filter: "",
	render: ({ options, categories, category, limits = {}, query, filter }) => html`
    <template layout="contents">
      <settings-page-layout layout="gap:4">
        ${store_default.ready(options) && html`
          <section layout="column gap:4" layout@768px="gap:5">
            <div layout="column gap" layout@992px="margin:bottom">
              <ui-text type="headline-m"> Tracker Database </ui-text>
              <ui-text type="body-l" mobile-type="body-m" color="secondary">
                Mind that not all listed activities are trackers, that is not all of them collect
                personal data.
              </ui-text>
              <ui-text type="label-m" mobile-type="body-m" color="brand-primary" underline>
                <a
                  href="https://github.com/ghostery/trackerdb"
                  rel="noreferrer"
                  layout="block"
                  target="_blank"
                >
                  Contribute to Ghostery Tracker Database on Github
                  <ui-icon name="arrow-right-s" layout="block inline margin:bottom:-2px"></ui-icon>
                </a>
              </ui-text>
            </div>
            <div layout="row:wrap gap items:center">
              <ui-button
                layout="width::12 grow"
                layout@768px="grow:0"
                onclick="${html.set("category", category !== "_all" ? "_all" : "")}"
                data-qa="button:trackers:expand"
              >
                <button>${category !== "_all" ? msg`Expand` : msg`Collapse`}</button>
              </ui-button>
              <ui-input layout="grow" layout@768px="grow:0">
                <select
                  value="${filter}"
                  onchange="${html.set("filter")}"
                  data-qa="select:trackers:filter"
                >
                  <option selected value="">Show all</option>
                  <option value="adjusted">
                    <!-- Plural form - list of adjusted trackers | tracker-list -->Adjusted
                  </option>
                  <option value="blocked">
                    <!-- Plural form - list of blocked trackers | tracker-list -->Blocked
                  </option>
                  <option value="trusted">
                    <!-- Plural form - list of trusted trackers | tracker-list -->Trusted
                  </option>
                </select>
              </ui-input>
              <ui-input layout="grow:5 width::250px" icon="search">
                <input
                  type="search"
                  defaultValue="${query}"
                  oninput="${setLazyQuery}"
                  placeholder="${msg`Search for a tracker or organization...`}"
                  data-qa="input:trackers:search"
                />
              </ui-input>
            </div>
            <div layout="column gap:0.5">
              ${store_default.ready(categories) && categories.map(({ id, key, description, trackers }) => html`
                  <settings-trackers-list
                    name="${key}"
                    description="${description}"
                    open="${isActive(category, key)}"
                    size="${trackers.length}"
                    adjusted="${trackers.filter((t) => options.exceptions[t.id]).length}"
                    ontoggle="${html.set("category", isActive(category, key) ? "" : key)}"
                    onclear="${clearCategory(id)}"
                  >
                    ${isActive(category, key) && html`
                      <ui-line></ui-line>
                      <div layout="column gap" layout@768px="padding:left:102px">
                        ${trackers.map((tracker, index) => index <= (limits[key] || PATTERNS_LIMIT) && html`
                              <div layout="row items:center gap">
                                <ui-action>
                                  <a
                                    href="${router_default.url(tracker_details_default, { tracker: tracker.id })}"
                                    layout="column grow basis:0"
                                    layout@768px="row gap:2"
                                    data-qa="button:trackers:details:${tracker.id}"
                                  >
                                    <ui-text type="label-m"> ${tracker.name} </ui-text>
                                    ${store_default.ready(tracker.organization) && html`
                                      <ui-text color="secondary">
                                        ${tracker.organization.name}
                                      </ui-text>
                                    `}
                                  </a>
                                </ui-action>
                                <div layout="row items:center gap">
                                  ${options.exceptions[tracker.id] && html`
                                    <ui-text type="label-s" color="secondary">
                                      <!-- Singular form - tracker has been adjusted | tracker -->adjusted
                                    </ui-text>
                                  `}
                                  <settings-exception-toggle
                                    value="${isTrusted(options, tracker)}"
                                    responsive
                                    onchange="${toggleException(tracker)}"
                                    layout="shrink:0"
                                  ></settings-exception-toggle>
                                </div>
                              </div>
                            `.key(tracker.id))}
                      </div>
                      ${(limits[key] || PATTERNS_LIMIT) < trackers.length && html`
                        <div layout="row center margin:bottom:2">
                          <ui-button onclick="${loadMore(key)}">
                            <button>Load more</button>
                          </ui-button>
                        </div>
                      `}
                    `}
                  </settings-trackers-list>
                `.key(key))}
            </div>
          </section>
        `}
      </settings-page-layout>
    </template>
  `
};
//#endregion
export { trackers_default as default };
