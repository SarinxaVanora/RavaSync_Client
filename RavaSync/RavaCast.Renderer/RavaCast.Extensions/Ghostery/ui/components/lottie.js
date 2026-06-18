import { html } from "../../npm/hybrids/src/template/index.js";
import { lottie } from "../../npm/lottie-web/build/player/esm/lottie_light.min.js";
//#region src/ui/components/lottie.js
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
var lottie_default = {
	src: "",
	autoplay: false,
	playOnHover: {
		value: "",
		connect: (host, key) => {
			const el = host[key] && host.closest(`#${host[key]}`);
			if (el) {
				const onmouseenter = () => host.lottie.play();
				const onmouseleave = () => host.lottie.pause();
				el.addEventListener("mouseenter", onmouseenter);
				el.addEventListener("mouseleave", onmouseleave);
				return () => {
					el.removeEventListener("mouseenter", onmouseenter);
					el.removeEventListener("mouseleave", onmouseleave);
				};
			}
		}
	},
	lottie: {
		value: (host) => lottie.loadAnimation({
			container: host,
			renderer: "svg",
			loop: true,
			autoplay: host.autoplay,
			path: chrome.runtime.getURL(host.src)
		}),
		observe() {}
	},
	render: () => {
		return html`<template layout="block"> </template>`;
	}
};
//#endregion
export { lottie_default as default };
