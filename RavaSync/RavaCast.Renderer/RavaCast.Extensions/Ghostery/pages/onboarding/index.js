import mount from "../../npm/hybrids/src/mount.js";
import store_default from "../../npm/hybrids/src/store.js";
import router_default from "../../npm/hybrids/src/router.js";
import { html } from "../../npm/hybrids/src/template/index.js";
import ManagedConfig from "../../store/managed-config.js";
import Options from "../../store/options.js";
import "../../ui/index.js";
import { HOME_PAGE_URL } from "../../utils/urls.js";
import success_default from "./views/success.js";
import modes_default from "./views/modes.js";
import main_default from "./views/main.js";
import "./elements.js";
//#region src/pages/onboarding/index.js
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
Promise.all([store_default.resolve(Options), store_default.resolve(ManagedConfig)]).then(([{ terms }, managedConfig]) => {
	if (managedConfig.disableOnboarding || terms && managedConfig.disableUserControl) return window.location.replace(HOME_PAGE_URL);
	store_default.set(Options, { onboarding: true });
	mount(document.body, {
		stack: router_default(terms ? [success_default, modes_default] : [
			main_default,
			modes_default,
			success_default
		]),
		render: ({ stack }) => html`
        <template layout="grid height::100%">
          <ui-page-layout>${stack}</ui-page-layout>
        </template>
      `
	});
	setInterval(() => chrome.runtime.sendMessage({ action: "keepAlive" }), 15e3);
});
//#endregion
