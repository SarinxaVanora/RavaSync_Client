import store_default from "../../../npm/hybrids/src/store.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import ManagedConfig from "../../../store/managed-config.js";
import Options from "../../../store/options.js";
//#region src/pages/settings/views/additional-filters.js
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
function getAdditionalFiltersLabel(options) {
	const labels = [];
	if (options.regionalFilters.enabled) labels.push(msg`Regional Block Lists`);
	if (options.customFilters.enabled) labels.push(msg`Custom Filters`);
	if (!labels.length) return msg`No options are currently enabled`;
	return msg`Enabled: ${labels.join(", ")}`;
}
var additional_filters_default = {
	options: store_default(Options),
	managedConfig: store_default(ManagedConfig),
	render: ({ options, managedConfig }) => html`
    <template layout="contents">
      <settings-page-layout layout="column gap:4">
        ${store_default.ready(options) && html`
          <section layout="column gap:4">
            <div layout="column gap" layout@992px="margin:bottom">
              <settings-back-button></settings-back-button>
              <ui-text type="headline-m">Additional Filters</ui-text>
              <div layout="column gap:0.5">
                <ui-text type="body-l" mobile-type="body-m" color="secondary">
                  Extend your protection with regional and custom filters for advanced control.
                </ui-text>
              </div>
            </div>
            <div layout="column gap">
              <settings-regional-filters></settings-regional-filters>
              <settings-managed value="${managedConfig.customFilters.enabled}">
                <settings-custom-filters></settings-custom-filters>
              </settings-managed>
            </div>
          </section>
        `}
      </settings-page-layout>
    </template>
  `
};
//#endregion
export { additional_filters_default as default, getAdditionalFiltersLabel };
