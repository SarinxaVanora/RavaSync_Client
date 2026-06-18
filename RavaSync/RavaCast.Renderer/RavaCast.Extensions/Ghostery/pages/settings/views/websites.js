import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import Options, { GLOBAL_PAUSE_ID } from "../../../store/options.js";
import { numberFormatter } from "../../../ui/labels.js";
import { ACTION_PAUSE_ASSISTANT } from "../../../npm/@ghostery/config/dist/esm/actions.js";
import Config, { dismissAction } from "../../../store/config.js";
import "../assets/no_websites.js";
import ElementPickerSelectors from "../../../store/element-picker-selectors.js";
import website_details_default from "./website-details.js";
import websites_add_default from "./websites-add.js";
import whotracksme_default from "./whotracksme.js";
//#region src/pages/settings/views/websites.js
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
function revokeCallback(item) {
	return ({ options }, event) => {
		event.preventDefault();
		event.stopPropagation();
		const exceptions = Array.from(item.exceptions).reduce((acc, id) => {
			const exception = options.exceptions[id];
			const domains = exception.domains.filter((d) => d !== item.id);
			acc[id] = exception.global || domains.length > 0 ? {
				...exception,
				domains
			} : null;
			return acc;
		}, {});
		if (item.assist) dismissAction(item.id, ACTION_PAUSE_ASSISTANT);
		store_default.set(ElementPickerSelectors, { hostnames: { [item.id]: null } });
		if (options.mode === "default") store_default.set(options, {
			paused: { [item.id]: null },
			exceptions
		});
		else if (options.mode === "zap") store_default.set(options, {
			zapped: { [item.id]: null },
			exceptions
		});
	};
}
var websites_default = {
	[router_default.connect]: { stack: [website_details_default, websites_add_default] },
	config: store_default(Config),
	options: store_default(Options),
	elementPickerSelectors: store_default(ElementPickerSelectors),
	query: "",
	websites: ({ config, options, elementPickerSelectors, query }) => {
		if (!store_default.ready(config, options, elementPickerSelectors)) return [];
		query = query.toLowerCase().trim();
		let websites;
		if (options.mode === "default") websites = Object.entries(options.paused).filter(({ id }) => id !== GLOBAL_PAUSE_ID).filter(([id, { assist }]) => !assist || assist && config.hasAction(id, "pause-assistant")).map(([id, { revokeAt, assist, managed }]) => ({
			id,
			revokeAt,
			assist,
			managed,
			exceptions: /* @__PURE__ */ new Set(),
			counter: 0
		}));
		else if (options.mode === "zap") websites = Object.keys(options.zapped).map((id) => ({
			id,
			exceptions: /* @__PURE__ */ new Set(),
			counter: 0
		}));
		Object.entries(elementPickerSelectors.hostnames).forEach(([domain, list]) => {
			const website = websites.find((e) => e.id === domain);
			if (website) website.counter += list.length;
			else if (options.mode === "default") websites.push({
				id: domain,
				exceptions: /* @__PURE__ */ new Set(),
				counter: list.length
			});
		});
		Object.entries(options.exceptions).forEach(([id, { domains }]) => {
			domains.forEach((domain) => {
				const website = websites.find((e) => e.id === domain);
				if (website) {
					website.exceptions.add(id);
					website.counter += 1;
				} else if (options.mode === "default") websites.push({
					id: domain,
					exceptions: new Set([id]),
					counter: 1
				});
			});
		});
		return websites.filter(({ id }) => id.includes(query)).sort((a, b) => {
			if (a.assist !== b.assist) return Number(a.assist) - Number(b.assist);
			if (a.id < b.id) return -1;
			if (a.id > b.id) return 1;
			return 0;
		});
	},
	assistWebsites: ({ websites }) => websites.filter(({ assist, managed, counter }) => assist && !managed && counter === 0),
	userWebsites: ({ websites, assistWebsites }) => websites.filter((item) => !assistWebsites.includes(item)),
	render: ({ websites, assistWebsites, userWebsites, query }) => html`
    <template layout="contents">
      <settings-page-layout layout="gap:4">
        <div layout="column gap" layout@992px="margin:bottom">
          <div layout="row items:center content:space-between">
            <ui-text type="headline-m">Websites</ui-text>
          </div>
          <ui-text type="body-l" mobile-type="body-m" color="secondary">
            All websites with adjusted protection status will be listed here.
          </ui-text>
        </div>
        <section layout="column gap:4" layout@768px="gap:5">
          <div layout="row items:center gap:2">
            <ui-input icon="search" layout="grow:1">
              <input
                type="search"
                value="${query}"
                placeholder="${msg`Search website...`}"
                oninput="${html.set("query")}"
              />
            </ui-input>
            <ui-button>
              <a href="${router_default.url(websites_add_default)}">Add</a>
            </ui-button>
          </div>
          <div layout="column gap">
            ${!websites.length && !query && html`
              <div layout="block:center width:::400px margin:2:auto" layout@768px="margin:top:4">
                <img src="${"data:image/svg+xml,%3csvg%20width='128'%20height='128'%20viewBox='0%200%20128%20128'%20fill='none'%20xmlns='http://www.w3.org/2000/svg'%3e%3cpath%20d='M16%2016L112%20112'%20stroke='%239BA3B4'%20stroke-width='4'%20stroke-linecap='round'%20stroke-linejoin='round'%20/%3e%3cpath%20d='M113.166%2049.25H68.3332M14.833%2049.25H47.5M105.113%20106.642C101.957%20108.25%2097.8273%20108.25%2089.5664%20108.25H38.433C30.1722%20108.25%2026.0418%20108.25%2022.8867%20106.642C20.1112%20105.228%2017.8548%20102.972%2016.4407%20100.196C14.833%2097.041%2014.833%2092.911%2014.833%2084.65V43.35C14.833%2035.0892%2014.833%2030.9588%2016.4407%2027.8036C17.5187%2025.6879%2019.0862%2023.8737%2021%2022.5044M37.5%2019.75H89.5664C97.8273%2019.75%20101.957%2019.75%20105.113%2021.3577C107.888%2022.7718%20110.145%2025.0282%20111.559%2027.8036C113.166%2030.9589%20113.166%2035.0892%20113.166%2043.35V84.65C113.166%2086.2467%20113.166%2089.6891%20113.155%2091'%20stroke='%239BA3B4'%20stroke-width='4'%20stroke-linecap='round'%20stroke-linejoin='round'%20/%3e%3c/svg%3e"}" layout="size:96px" layout@768px="size:128px" />
              </div>
            `}
            ${userWebsites.length > 0 && html`
              <settings-option>
                Manually Adjusted
                <settings-table responsive slot="card-footer">
                  <div slot="header" layout="column" layout@768px="grid:3fr|4fr|1fr|50px gap:2">
                    <ui-text type="label-m">Website <span>(${websites.length})</span></ui-text>
                    <ui-text type="label-m" layout="hidden" layout@768px="block">
                      Protection status
                    </ui-text>
                    <ui-text type="label-m" layout="hidden" layout@768px="block"
                      >Exceptions</ui-text
                    >
                  </div>
                  ${userWebsites.map((item) => html`
                      <ui-action layout="block" data-qa="component:website:${item.id}">
                        <a
                          href="${router_default.url(website_details_default, { domain: item.id })}"
                          layout="grid:1|min:auto gap:2 items:center:stretch margin:-2:0 padding:2:0"
                          layout@768px="grid:3fr|4fr|1fr|50px gap:2"
                        >
                          <ui-text type="label-l" ellipsis> ${item.id} </ui-text>
                          ${!item.managed && html`
                            <ui-action>
                              <button
                                layout@768px="order:1 row center"
                                onclick="${revokeCallback(item)}"
                                data-qa="button:website:trash:${item.id}"
                              >
                                <ui-icon name="trash" layout="size:3" color="tertiary"></ui-icon>
                              </button>
                            </ui-action>
                          `}
                          <ui-line layout="area:2" layout@768px="hidden"></ui-line>
                          <settings-protection-status
                            layout@768px="grow"
                            revokeAt="${item.revokeAt}"
                            assist="${item.assist}"
                          ></settings-protection-status>
                          <div
                            layout="row items:center gap self:center"
                            layout@768px="grow self:auto"
                          >
                            <ui-text type="label-m">
                              ${item.counter ? numberFormatter.format(item.counter) : ""}
                            </ui-text>
                          </div>
                        </a>
                      </ui-action>
                    `)}
                </settings-table>
              </settings-option>
            `}
            ${assistWebsites.length > 0 && html`
              <settings-option>
                Automatically Adjusted
                <span slot="description">
                  Paused automatically by Ghostery’s Browsing Assistant to prevent ad blocker
                  breakage.
                </span>
                <ui-text slot="footer" color="tertiary" underline>
                  ${msg.html`You can disable it in <a href="${router_default.url(whotracksme_default)}">WhoTracks.Me settings</a>.`}
                </ui-text>

                <settings-table responsive slot="card-footer">
                  <div slot="header" layout="column" layout@768px="grid:3fr|60px gap:4">
                    <ui-text type="label-m">Website <span>(${websites.length})</span></ui-text>
                  </div>
                  ${assistWebsites.map((item) => html`
                      <ui-action layout="block" data-qa="component:website:${item.id}">
                        <a
                          href="${router_default.url(website_details_default, { domain: item.id })}"
                          layout="grid:1|min:auto gap:2 items:center:stretch margin:-2:0 padding:2:0"
                          layout@768px="grid:3fr|60px gap:4"
                        >
                          <ui-text type="label-l" ellipsis>${item.id}</ui-text>
                          <ui-action>
                            <button
                              layout@768px="order:1 row center"
                              onclick="${revokeCallback(item)}"
                              data-qa="button:website:trash:${item.id}"
                            >
                              <ui-icon name="trash" layout="size:3" color="tertiary"></ui-icon>
                            </button>
                          </ui-action>
                        </a>
                      </ui-action>
                    `)}
                </settings-table>
              </settings-option>
            `}
          </div>
        </section>
      </settings-page-layout>
    </template>
  `
};
//#endregion
export { websites_default as default };
