import store_default from "../../../npm/hybrids/src/store.js";
import router_default from "../../../npm/hybrids/src/router.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import { SUPPORT_PAGE_URL } from "../../../utils/urls.js";
import { getCurrentTab, openHref } from "../../../utils/tabs.js";
import report_confirm_default from "./report-confirm.js";
//#region src/pages/panel/views/report-form.js
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
var Form = {
	url: "",
	category: "",
	email: "",
	description: "",
	screenshot: true,
	[store_default.connect]: {
		async get() {
			const currentTab = await getCurrentTab();
			const url = currentTab && new URL(currentTab.url);
			return { url: url ? `${url.origin}${url.pathname}` : "" };
		},
		async set(_, values) {
			const error = await chrome.runtime.sendMessage({
				action: "report-broken-page",
				tab: await getCurrentTab(),
				...values
			});
			if (error) throw new Error(error);
			return values;
		}
	}
};
function submit(host, event) {
	try {
		router_default.resolve(event, store_default.submit(host.form).then(() => store_default.clear(Form)));
	} catch {
		event.preventDefault();
	}
}
var report_form_default = {
	category: {
		value: "",
		observe: (host, category) => store_default.set(host.form, { category })
	},
	form: store_default(Form, { draft: true }),
	render: ({ form }) => html`
    <template layout="column grow shrink">
      <ui-header>
        <ui-text type="label-m" layout="row gap items:center"> Report an issue </ui-text>
        <ui-action slot="actions">
          <a href="${router_default.backUrl()}">
            <ui-icon name="close" color="primary" layout="size:3"></ui-icon>
          </a>
        </ui-action>
      </ui-header>
      <panel-container layout="width:::100cqw">
        ${store_default.ready(form) && html`
          <form
            layout="column gap:2 padding:2"
            onsubmit="${submit}"
            action="${router_default.url(report_confirm_default)}"
          >
            ${store_default.error(form) && !store_default.pending(form) && html`
              <div layout="row gap">
                <ui-icon name="warning" layout="inline size:2" color="danger-primary"></ui-icon>
                <ui-text type="body-s" color="danger-primary">
                  ${store_default.error(form)?.message}
                </ui-text>
              </div>
            `}
            <ui-text type="body-s">
              Tell us what went wrong on this page and we’ll look into it.
            </ui-text>
            <ui-text
              type="label-s"
              color="brand-primary"
              style="word-break: break-all"
              layout="row gap:0.5 items:center"
            >
              <ui-icon name="globe"></ui-icon> ${form.url}
            </ui-text>
            <ui-input>
              <select
                value="${form.category}"
                onchange="${html.set(form, "category")}"
                required
                layout="::font:body-s"
              >
                <option value="cookie-banner">Cookie banner visible</option>
                <option value="ads-showing">Ads are showing</option>
                <option value="page-frozen">Page frozen</option>
                <option value="adblocker-detected">Ad blocker detected</option>
                <option value="layout-broken">Layout broken</option>
                <option value="">Other</option>
              </select>
            </ui-input>

            <ui-input>
              <input
                type="email"
                name="email"
                placeholder="${msg`Email address (optional)`}"
                layout="::font:body-s"
                value="${form.email}"
                oninput="${html.set(form, "email")}"
              />
            </ui-input>
            ${!form.category && html` <ui-input>
              <textarea
                placeholder="${msg`Please describe the issue you’re experiencing`}"
                rows="5"
                autocomplete="off"
                style="resize: vertical"
                oninput="${html.set(form, "description")}"
                maxlength="4000"
                layout="::font:body-s"
                required="${!form.screenshot}"
              ></textarea>
            </ui-input>`}
            <label layout="row gap items:center">
              <ui-input>
                <input
                  type="checkbox"
                  checked="${form.screenshot}"
                  onchange="${html.set(form, "screenshot")}"
                />
              </ui-input>
              <ui-text type="body-s"> Include a screenshot of the current page </ui-text>
            </label>
            <panel-card layout="padding">
              <ui-text type="body-s" color="secondary">
                Want to report an issue on a different page?
              </ui-text>
              <ui-text type="body-s" color="secondary" underline>
                ${msg.html`
                  Please go to <a href="${SUPPORT_PAGE_URL}" onclick="${openHref}">Ghostery Support</a>
                `}
              </ui-text>
            </panel-card>
            <div layout="grid:2 gap:1 margin:top">
              <ui-button disabled="${store_default.pending(form)}">
                <a href="${router_default.backUrl()}">Cancel</a>
              </ui-button>
              <ui-button type="primary" disabled="${store_default.pending(form)}">
                <button type="submit">Send</button>
              </ui-button>
            </div>
          </form>
        `}
      </panel-container>
    </template>
  `
};
//#endregion
export { report_form_default as default };
