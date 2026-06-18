import store_default from "../../../npm/hybrids/src/store.js";
import { msg } from "../../../npm/hybrids/src/localize.js";
import { isEdge, isMobile, isOpera, isSafari } from "../../../utils/browser-info.js";
import Options from "../../../store/options.js";
import { BECOME_A_CONTRIBUTOR_PAGE_URL, PANEL_STORE_PAGE_URL } from "../../../utils/urls.js";
import { isSerpSupported } from "../../../utils/opera.js";
import call_for_review_default from "../assets/call-for-review.js";
import edge_mobile_qr_code_default from "../assets/edge-mobile-qr-code.js";
//#region src/pages/panel/store/notification.js
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
var NOTIFICATIONS = {
	terms: {
		icon: "triangle",
		type: "danger",
		text: msg`Due to browser restrictions and additional permissions missing, Ghostery is not able to protect you.`,
		url: isSafari() ? "https://www.ghostery.com/blog/how-to-install-extensions-in-safari?utm_source=gbe&utm_campaign=safaripermissions" : "https://www.ghostery.com/support?utm_source=gbe&utm_campaign=permissions",
		action: msg`Get help`
	},
	opera: {
		icon: "logo-opera",
		type: "danger",
		text: msg`Expand Ghostery ad blocking to search engines in a few easy steps.`,
		url: "https://www.ghostery.com/blog/block-search-engine-ads-on-opera-guide?utm_source=gbe&utm_campaign=opera_serp",
		action: msg`Enable Ad Blocking Now`
	},
	edgeMobile: {
		img: edge_mobile_qr_code_default,
		type: "image",
		text: msg`Android and iPhone just got a new Edge. Ghostery included.`,
		url: "https://www.ghostery.com/download/edge-mobile?utm_source=gbe&utm_campaign=panel",
		action: msg`Scan and take Ghostery from desktop to mobile`
	},
	review: {
		img: call_for_review_default,
		type: "review",
		text: msg`We're so glad Ghostery has your heart! Help others find us too - it only takes a moment.`,
		url: PANEL_STORE_PAGE_URL,
		action: msg`Leave a review today`
	},
	contribution: {
		icon: "heart",
		type: "",
		text: msg`Hey, do you enjoy Ghostery and want to support our work?`,
		url: `${BECOME_A_CONTRIBUTOR_PAGE_URL}?utm_source=gbe&utm_campaign=panel-becomeacontributor`,
		action: msg`Become a Contributor`
	}
};
var randomize = Math.random();
var Notification = {
	icon: "",
	img: "",
	type: "",
	text: "",
	url: "",
	action: "",
	[store_default.connect]: async () => {
		const { terms, panel } = await store_default.resolve(Options);
		if (!terms) return NOTIFICATIONS.terms;
		if (isOpera() && !await isSerpSupported()) return NOTIFICATIONS.opera;
		if (!panel.notifications) return null;
		if (isEdge() && !isMobile()) return NOTIFICATIONS.edgeMobile;
		if (randomize < .5) return NOTIFICATIONS.review;
		return NOTIFICATIONS.contribution;
	}
};
//#endregion
export { Notification as default };
