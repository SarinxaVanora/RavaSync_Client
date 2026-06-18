import mount from "../../npm/hybrids/src/mount.js";
import store_default from "../../npm/hybrids/src/store.js";
import { html } from "../../npm/hybrids/src/template/index.js";
import Options from "../../store/options.js";
import "../../ui/index.js";
import { setupNotificationPage } from "../../utils/notifications.js";
//#region src/pages/notifications/pause-resume.js
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
var close = setupNotificationPage(390);
var domain = new URLSearchParams(window.location.search).get("domain");
async function resume() {
	await store_default.set(Options, { paused: { [domain]: null } });
	close({ reload: true });
}
mount(document.body, { render: () => html`
    <template layout="block overflow">
      <ui-notification icon="protection-l">
        <div layout="column gap">
          <ui-text type="label-m">
            Ghostery users report that ad blockers are no longer breaking this site. Ghostery will
            be re-enabled.
          </ui-text>
          <ui-text type="body-s"> Tracker & ad blocking will be active on this page. </ui-text>
        </div>
        <div layout="row gap" layout="width::10">
          <ui-button type="success" onclick="${resume}" data-qa="button:dismiss">
            <button>OK</button>
          </ui-button>
        </div>
      </ui-notification>
    </template>
  ` });
//#endregion
