import { html } from "../../../npm/hybrids/src/template/index.js";
//#region src/pages/panel/components/badge.js
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
var badge_default = {
	type: {
		value: "",
		reflect: true
	},
	render: () => html`
    <template layout="row inline center padding:0.5">
      <ui-text type="label-s" color="inherit"><slot></slot></ui-text>
    </template>
  `.css`
    :host {
      background: var(--background-secondary);
      color: var(--color-tertiary);
      border-radius: 8px;
    }

    :host([type='danger']) {
      color: var(--color-danger-primary);
      background: var(--background-danger-primary);
    }

    :host([type='brand']) {
      color: var(--color-brand-primary);
      background: var(--background-brand-primary);
    }
  `
};
//#endregion
export { badge_default as default };
