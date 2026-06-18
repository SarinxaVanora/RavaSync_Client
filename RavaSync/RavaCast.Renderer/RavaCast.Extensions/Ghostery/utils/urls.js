import { isSafari } from "./browser-info.js";
//#region src/utils/urls.js
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
var GHOSTERY_DOMAIN = "ghostery.com";
var TERMS_AND_CONDITIONS_URL = `https://www.${GHOSTERY_DOMAIN}/privacy/ghostery-terms-and-conditions?utm_source=gbe&utm_campaign=onboarding`;
var HOME_PAGE_URL = `https://www.${GHOSTERY_DOMAIN}/`;
var WTM_PAGE_URL = `https://www.${GHOSTERY_DOMAIN}/whotracksme`;
var SUPPORT_PAGE_URL = `https://www.${GHOSTERY_DOMAIN}/support`;
var WHATS_NEW_PAGE_URL = `https://www.${GHOSTERY_DOMAIN}/blog/ghostery-extension-v10-5?embed=1&utm_campaign=whatsnew`;
var PANEL_STORE_PAGE_URL = `${HOME_PAGE_URL}downloads/review?utm_source=gbe&utm_campaign=panel`;
var REVIEW_STORE_PAGE_URL = `${HOME_PAGE_URL}downloads/review?utm_source=gbe&utm_campaign=review`;
var BECOME_A_CONTRIBUTOR_PAGE_URL = isSafari() ? "ghosteryapp://www.ghostery.com" : "https://www.ghostery.com/become-a-contributor";
var ENGINE_CONFIGS_ROOT_URL = `https://cdn.ghostery.com/adblocker/configs`;
var CDN_URL = "https://cdn.ghostery.com/";
var PAUSE_ASSISTANT_LEARN_MORE_URL = `https://www.${GHOSTERY_DOMAIN}/blog/browsing-assistant-user-agent`;
var TRACKERS_PREVIEW_LEARN_MORE_URL = `https://www.${GHOSTERY_DOMAIN}/blog/introducing-wtm-serp-report`;
var ZAP_AUTORELOAD_DISABLED_HOSTNAMES = ["youtube.com"];
//#endregion
export { BECOME_A_CONTRIBUTOR_PAGE_URL, CDN_URL, ENGINE_CONFIGS_ROOT_URL, GHOSTERY_DOMAIN, HOME_PAGE_URL, PANEL_STORE_PAGE_URL, PAUSE_ASSISTANT_LEARN_MORE_URL, REVIEW_STORE_PAGE_URL, SUPPORT_PAGE_URL, TERMS_AND_CONDITIONS_URL, TRACKERS_PREVIEW_LEARN_MORE_URL, WHATS_NEW_PAGE_URL, WTM_PAGE_URL, ZAP_AUTORELOAD_DISABLED_HOSTNAMES };
