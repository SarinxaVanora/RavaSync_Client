import store_default from "../../../npm/hybrids/src/store.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import { getBrowser, isMobile } from "../../../utils/browser-info.js";
import Options from "../../../store/options.js";
import { lang } from "../../../ui/labels.js";
import "../../../ui/assets/lottie-mode-zap.json.js";
import "../assets/success-default.js";
import pin_extension_chrome_default from "../assets/pin-extension-chrome.js";
import pin_extension_edge_default from "../assets/pin-extension-edge.js";
import pin_extension_opera_default from "../assets/pin-extension-opera.js";
//#region src/pages/onboarding/views/success.js
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
var screenshotURL = "";
var type = "";
{
	const { name } = getBrowser();
	if (name === "chrome" || name === "yandex" || name === "oculus") {
		screenshotURL = pin_extension_chrome_default;
		type = "chrome";
	} else if (name === "edge" && !isMobile()) {
		screenshotURL = pin_extension_edge_default;
		type = "edge";
	} else if (name === "opera") {
		screenshotURL = pin_extension_opera_default;
		type = "opera";
	} else if (name === "brave") {
		screenshotURL = pin_extension_chrome_default;
		type = "brave";
	}
}
var success_default = {
	options: store_default(Options),
	render: {
		connect: () => {
			chrome.runtime.sendMessage({
				action: "telemetry:ping",
				event: "install_complete"
			});
		},
		value: ({ options }) => html`
      <template layout="column gap:2 width:::500px">
        <ui-card data-qa="view:success">
          <section layout="block:center column gap:2">
            ${options.mode === "default" && html`
              <div layout="row center">
                <img src="${"/assets/success-default-cxtFo0JP.svg"}" layout="size:20" />
              </div>
              <ui-text type="display-s">Setup Successful</ui-text>
              <ui-text>
                Ghostery is all set to stop trackers in their tracks and protect your privacy while
                browsing!
              </ui-text>
            `}
            ${options.mode === "zap" && html`
              <ui-text type="display-s">You’re ready to block ads</ui-text>
              <div
                layout="block:left column gap padding:1:5 relative"
                layout@520px="block:center grid:3 padding:1:0"
              >
                <div
                  layout="hidden"
                  layout@520px="block absolute top:28px left:100px right:100px height:4px"
                  style="
                    border-top:3px dashed var(--background-primary);
                    background:
                      linear-gradient(var(--background-primary) 0 0) padding-box,
                      linear-gradient(to right, var(--background-brand-primary), var(--background-danger-primary), var(--background-success-primary));
                  "
                ></div>
                <div layout="row items:center gap" layout@520px="column">
                  <onboarding-step number="1" icon="websites" type="brand"> </onboarding-step>
                  <ui-text type="label-m">Open a site</ui-text>
                </div>
                <div layout="row items:center gap" layout@520px="column">
                  <onboarding-step number="2" icon="block-m" type="danger"> </onboarding-step>
                  ${lang === "en" ? html`<ui-text type="label-m" translate="no"> Zap ads once </ui-text>` : html`<ui-text type="label-m">Block ads once</ui-text>`}
                </div>
                <div layout="row items:center gap" layout@520px="column">
                  <onboarding-step number="3" icon="trust-m" type="success"> </onboarding-step>
                  <ui-text type="label-m"> Site stays ad-free every time you visit </ui-text>
                </div>
              </div>
              <div layout="row center">
                <ui-lottie
                  src="${"/assets/lottie-mode-zap-Dd1fuCpJ.json"}"
                  autoplay
                  style="border-radius:8px"
                  layout="ratio:83/45 width:100%::400px overflow"
                ></ui-lottie>
              </div>
            `}
          </section>
        </ui-card>
        ${screenshotURL && html`
          <ui-card>
            <section layout="column center gap:3">
              <div layout="block:center column gap" layout@520px="padding:1:2:0">
                <ui-text type="display-s"> Pin extension for easy access </ui-text>
                <ui-text layout="padding:0:2">
                  ${msg.html`Click the puzzle icon next to the search bar and <strong>pin Ghostery</strong> to your toolbar.`}
                </ui-text>
              </div>
              <img
                src="${screenshotURL}"
                layout="width:full::400px"
                style="border-radius:8px; overflow:hidden;"
              />
            </section>
          </ui-card>
          <onboarding-pin-it browser="${type}"> Pin it here </onboarding-pin-it>
        `}
      </template>
    `
	}
};
//#endregion
export { success_default as default };
