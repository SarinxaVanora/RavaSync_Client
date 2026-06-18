import { html } from "../../../npm/hybrids/src/template/index.js";
//#region src/pages/panel/components/card.js
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
var card_default = {
	type: {
		value: "",
		reflect: true
	},
	render: () => html`
    <template layout="block padding:1.5">
      <slot></slot>
    </template>
  `.css`
    :host {
      background: var(--background-secondary);
      border-radius: 8px;
    }

    :host([type="info"]) {
      background: var(--background-brand-primary);
    }
  `
};
//#endregion
export { card_default as default };
