import store_default from "../../../npm/hybrids/src/store.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import REGIONS from "../../../utils/regions.js";
import Options from "../../../store/options.js";
import { languages } from "../../../ui/labels.js";
//#region src/pages/settings/views/regional-filters.js
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
function setRegion(id) {
	return ({ options }, event) => {
		const set = new Set(options.regionalFilters.regions);
		if (event.target.checked) set.add(id);
		else set.delete(id);
		store_default.set(options, { regionalFilters: { regions: [...set].sort() } });
	};
}
var regional_filters_default = {
	options: store_default(Options),
	render: ({ options }) => html`
    <template layout="contents">
      <settings-toggle
        value="${options.regionalFilters.enabled}"
        onchange="${html.set(options, "regionalFilters.enabled")}"
        icon="pin"
        data-qa="toggle:regional-filters"
      >
        Regional Block Lists
        <span slot="description">
          Blocks additional ads, trackers, and pop-ups specific to the language of websites you
          visit. Enable only the languages you need to avoid slowing down your browser.
        </span>
        ${options.regionalFilters.enabled && html`
          <div slot="card-footer" layout="grid:repeat(auto-fill,minmax(140px,1fr)) gap:1:0.5">
            ${REGIONS.map((id) => html`
                <label layout="row gap items:center ::user-select:none padding:0.5">
                  <ui-input>
                    <input
                      type="checkbox"
                      disabled="${!options.regionalFilters.enabled}"
                      checked="${options.regionalFilters.regions.includes(id)}"
                      onchange="${setRegion(id)}"
                      data-qa="checkbox:regional-filters:${id}"
                    />
                  </ui-input>
                  <ui-text type="body-s" color="secondary">
                    ${languages.of(id.toUpperCase())} (${id})
                  </ui-text>
                </label>
              `)}
          </div>
        `}
      </settings-toggle>
    </template>
  `
};
//#endregion
export { regional_filters_default as default };
