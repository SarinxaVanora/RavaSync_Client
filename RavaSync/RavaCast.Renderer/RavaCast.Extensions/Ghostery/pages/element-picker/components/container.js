import { html } from "../../../npm/hybrids/src/template/index.js";
//#region src/pages/element-picker/components/container.js
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
var container_default = { render: () => html`
    <template layout="block">
      <slot></slot>
    </template>
  `.css`
    :host {
      background: var(--background-secondary);
      border-top: 1px solid var(--border-primary);
    }
  ` };
//#endregion
export { container_default as default };
