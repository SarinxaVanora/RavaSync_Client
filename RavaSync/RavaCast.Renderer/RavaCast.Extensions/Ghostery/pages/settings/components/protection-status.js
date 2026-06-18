import { html } from "../../../npm/hybrids/src/template/index.js";
//#region src/pages/settings/components/protection-status.js
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
var protection_status_default = {
	revokeAt: void 0,
	assist: false,
	render: ({ revokeAt, assist }) => html`
    <template layout="row items:center gap">
      ${revokeAt === void 0 && html`<settings-badge type="brand" uppercase>Active</settings-badge>`}
      ${revokeAt !== void 0 && !assist && html`
        <settings-badge type="warning" uppercase>Paused</settings-badge>
        <ui-text color="secondary" layout="grow">
          <ui-revoke-at revokeAt="${revokeAt}"></ui-revoke-at>
        </ui-text>
      `}
      ${revokeAt !== void 0 && assist && html`
        <ui-tooltip autohide="5" delay="0">
          <div slot="content" layout="block:left padding:1:0.5">
            <ui-text type="label-s">Paused by Browsing Assistant</ui-text>
            <ui-text type="body-s"> Automatically paused to prevent adblocker breakage </ui-text>
          </div>
          <settings-badge type="pause-assistant" uppercase assist>
            Paused
            <ui-icon name="info"></ui-icon>
          </settings-badge>
        </ui-tooltip>
      `}
    </template>
  `
};
//#endregion
export { protection_status_default as default };
