import store_default from "../../../npm/hybrids/src/store.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import CustomFilters from "../../../store/custom-filters.js";
import Options from "../../../store/options.js";
//#region src/pages/settings/views/custom-filters.js
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
async function update(host, event) {
	const button = event.currentTarget;
	button.disabled = true;
	host.result = void 0;
	try {
		store_default.submit(host.input);
		host.result = await chrome.runtime.sendMessage({
			action: "customFilters:update",
			input: host.input.text
		});
	} finally {
		button.disabled = false;
	}
}
var custom_filters_default = {
	options: store_default(Options),
	input: store_default(CustomFilters, { draft: true }),
	result: void 0,
	render: ({ options, input, result }) => html`
    <template layout="contents">
      <settings-toggle
        value="${options.customFilters.enabled}"
        onchange="${html.set(options, "customFilters.enabled")}"
        icon="detailed-view"
        data-qa="toggle:custom-filters"
      >
        Custom Filters
        <span slot="description">
          Facilitates the creation of your own ad-blocking rules to customize your Ghostery
          experience.
        </span>
        <ui-text type="label-m" color="primary" slot="footer">
          <a
            href="https://github.com/ghostery/adblocker/wiki/Compatibility-Matrix"
            target="_blank"
            rel="noreferrer"
            layout="row items:center gap:2px"
          >
            Learn more on supported syntax
            <ui-icon name="chevron-right-s"></ui-icon>
          </a>
        </ui-text>
        ${options.customFilters.enabled && store_default.ready(input) && html`
          <div layout="column gap:2" slot="card-footer">
            <label layout="row gap items:center ::user-select:none">
              <ui-input>
                <input
                  type="checkbox"
                  checked="${options.customFilters.trustedScriptlets}"
                  onchange="${html.set(options, "customFilters.trustedScriptlets")}"
                  data-qa="checkbox:custom-filters:trusted-scriptlets"
                />
              </ui-input>
              <ui-text type="body-s">Allow trusted scriptlets</ui-text>
            </label>
            <ui-input>
              <textarea
                rows="10"
                autocomplete="off"
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                oninput="${html.set(input, "text")}"
                defaultValue="${input.text}"
                data-qa="input:custom-filters"
              ></textarea>
            </ui-input>

            <ui-button layout="self:start" onclick="${update}" data-qa="button:custom-filters:save">
              <button>Save</button>
            </ui-button>
            ${result && html`
              <div layout="column gap margin:top" data-qa="component:custom-filters:result">
                <div layout="column gap:0.5">
                  <ui-text type="label-s" color="secondary">
                    Custom filters have been updated
                  </ui-text>
                  <ui-text type="body-s" color="secondary">
                    ${html`
                          <details>
                            <summary>
                              <ui-text type="body-s" layout="inline" color="secondary">
                                DNR rules: ${result.dnrRules.length}
                              </ui-text>
                            </summary>
                            <ui-text type="body-s" color="secondary">
                              ${result.dnrRules.map((rule) => html`<pre>${JSON.stringify(rule, null, 2)}</pre>`)}
                            </ui-text>
                          </details>
                        `}
                  </ui-text>
                  <ui-text type="body-s" color="secondary">
                    Cosmetic filters: ${result.cosmeticFilters || 0}
                  </ui-text>
                </div>
              </div>
            `}
            ${!!result?.errors.length && html`
              <div layout="column gap:0.5" data-qa="component:custom-filters:errors">
                <ui-text type="label-s" color="secondary">
                  Errors (${result.errors.length})
                </ui-text>
                ${result?.errors.map((error) => html`<ui-text type="body-s" color="danger-secondary"> ${error} </ui-text>`)}
              </div>
            `}
          </div>
        `}
      </settings-toggle>
    </template>
  `
};
//#endregion
export { custom_filters_default as default };
