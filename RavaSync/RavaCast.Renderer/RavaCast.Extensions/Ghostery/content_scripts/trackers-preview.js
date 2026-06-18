(function() {
	//#region src/ui/categories.js
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
	var colors = {
		advertising: "#CB55CD",
		site_analytics: "#5EBEDB",
		consent: "#556FCD",
		essential: "#FC9734",
		utilities: "#FC9734",
		hosting: "#8459A5",
		customer_interaction: "#EF671E",
		unidentified: "#79859E",
		audio_video_player: "#4ECB4E",
		cdn: "#4ECBA1",
		comments: "#4EA1CB",
		email: "#4E4ECB",
		extensions: "#A14ECB",
		misc: "#CB4EA1",
		pornvertising: "#CB4E4E",
		social_media: "#CBA14E",
		telemetry: "#A1CB4E",
		other: "#D5DBE5"
	};
	var backgroundColors = {
		...colors,
		site_analytics: "#87D7EF",
		unidentified: "#DBDFE7"
	};
	var order = [
		"advertising",
		"site_analytics",
		"consent",
		"essential",
		"utilities",
		"hosting",
		"customer_interaction",
		"audio_video_player",
		"cdn",
		"comments",
		"email",
		"extensions",
		"misc",
		"pornvertising",
		"social_media",
		"telemetry",
		"unidentified",
		"other"
	];
	function getCategoryKey(category) {
		return colors[category] ? category : "unidentified";
	}
	function getCategoryBgColor(category) {
		return backgroundColors[getCategoryKey(category)];
	}
	//#endregion
	//#region src/ui/wheel.js
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
	function degToRad(degree) {
		return degree * (Math.PI / 180);
	}
	function grayscaleColor(hexColor) {
		const r = parseInt(hexColor.substr(1, 2), 16);
		const g = parseInt(hexColor.substr(3, 2), 16);
		const b = parseInt(hexColor.substr(5, 2), 16);
		const value = .2126 * r + .7152 * g + .0722 * b;
		return `rgb(${value}, ${value}, ${value})`;
	}
	function drawWheel(ctx, size, categories, { useScale = true, grayscale = false } = {}) {
		if (useScale && typeof window !== "undefined") {
			const { canvas } = ctx;
			canvas.style.width = size + "px";
			canvas.style.height = size + "px";
			const scale = window.devicePixelRatio;
			canvas.width = Math.floor(size * scale);
			canvas.height = Math.floor(size * scale);
			ctx.scale(scale, scale);
		}
		const groupedCategories = {};
		order.forEach((c) => groupedCategories[getCategoryKey(c)] = 0);
		categories.forEach((c) => groupedCategories[getCategoryKey(c)] += 1);
		const center = size / 2;
		const increment = 360 / categories.length;
		ctx.lineWidth = Math.floor(size * .14) * .95;
		const radius = size / 2 - ctx.lineWidth;
		ctx.strokeStyle = "blue";
		ctx.beginPath();
		ctx.arc(center, center, Math.floor(radius), 0, 2 * Math.PI);
		ctx.stroke();
		ctx.lineWidth = size * .14;
		let position = -90;
		for (const [category, numTrackers] of Object.entries(groupedCategories)) if (numTrackers > 0) {
			const newPosition = position + numTrackers * increment;
			const color = getCategoryBgColor(category);
			ctx.strokeStyle = grayscale ? grayscaleColor(color) : color;
			ctx.beginPath();
			ctx.arc(center, center, radius, degToRad(position), Math.min(degToRad(newPosition + 1), 2 * Math.PI));
			ctx.stroke();
			position = newPosition;
		}
	}
	//#endregion
	//#region src/content_scripts/trackers-preview.js
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
	var WRAPPER_CLASS = "wtm-popup-iframe-wrapper";
	var isMobile = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent);
	function closePopups() {
		[...document.querySelectorAll(`.${WRAPPER_CLASS}`)].forEach((popup) => {
			popup.parentElement.removeChild(popup);
		});
	}
	function resizePopup(height) {
		[...document.querySelectorAll(`.${WRAPPER_CLASS}`)].forEach((popup) => {
			popup.style.height = `${height}px`;
		});
	}
	var getTop = (el) => el.offsetTop + (el.offsetParent && getTop(el.offsetParent));
	function renderPopup(container, stats, popupUrl) {
		closePopups();
		const wrapper = document.createElement("div");
		wrapper.classList.add(WRAPPER_CLASS);
		if (isMobile) {
			wrapper.style.width = window.innerWidth - 20 + "px";
			wrapper.style.left = "10px";
		} else {
			const left = container.getBoundingClientRect().left - 350 / 2 + 12;
			wrapper.style.left = (left < 20 ? 20 : left) + "px";
		}
		wrapper.style.top = getTop(container) + 25 + "px";
		const iframe = document.createElement("iframe");
		iframe.setAttribute("src", `${popupUrl}?domain=${stats.domain}`);
		wrapper.appendChild(iframe);
		document.body.appendChild(wrapper);
	}
	function getWheelElement(stats, popupUrl) {
		const count = stats.stats.length;
		if (count === 0) return null;
		const container = document.createElement("div");
		container.classList.add("wtm-tracker-wheel-container");
		const label = document.createElement("div");
		label.innerText = count;
		const canvas = document.createElement("canvas");
		canvas.classList.add("wtm-tracker-wheel");
		drawWheel(canvas.getContext("2d"), 16, stats.stats);
		container.appendChild(canvas);
		container.appendChild(label);
		container.addEventListener("click", (ev) => {
			ev.preventDefault();
			ev.stopImmediatePropagation();
			renderPopup(container, stats, popupUrl);
		});
		return container;
	}
	var SELECTORS = [
		"[data-hveid] div.yuRUbf > div > span > a",
		"[data-hveid] div.yuRUbf > div > a",
		"[data-hveid] div.xpd a.cz3goc",
		"[data-hveid] > .xpd > div.kCrYT:first-child > a",
		"[data-hveid] div.OhZyZc > a",
		"li[data-id] h2 > a",
		"li[data-id] div.b_algoheader > a"
	].join(", ");
	function setupTrackersPreview(popupUrl) {
		const elements = [...window.document.querySelectorAll(SELECTORS)].filter((el) => !el.dataset.wtm);
		if (elements.length) {
			const links = elements.map((el) => {
				el.dataset.wtm = 1;
				if (el.hostname === window.location.hostname) {
					const url = new URL(el.href);
					if (url.pathname === "/url") return url.searchParams.get("url") || url.searchParams.get("q");
					if (url.pathname === "/ck/a" && url.searchParams.has("u")) try {
						const base64Str = url.searchParams.get("u").slice(2);
						return atob(base64Str) || "";
					} catch {
						return "";
					}
				}
				return el.href;
			});
			chrome.runtime.sendMessage({
				action: "getWTMReport",
				links
			}, (response) => {
				if (chrome.runtime.lastError) {
					console.error("Could not retrieve WTM information on URLs", chrome.runtime.lastError);
					return;
				}
				elements.forEach((anchor, i) => {
					const stats = response.wtmStats[i];
					if (stats) try {
						const wheelEl = getWheelElement(stats, popupUrl);
						if (!wheelEl) return;
						const container = anchor.parentElement.querySelector(".B6fmyf") || anchor.parentElement.parentElement.querySelector(".B6fmyf") || anchor.querySelector("span.yIn8Od") || anchor.querySelector("div[role=\"link\"]") || anchor.querySelector("div.UPmit.AP7Wnd") || anchor.parentElement.parentElement.querySelector(".b_tpcn");
						if (!container) return;
						if (container.classList.contains("b_tpcn")) container.style.display = "flex";
						let tempEl = container.firstElementChild;
						if (tempEl && tempEl.textContent.includes(stats.domain)) container.insertBefore(wheelEl, tempEl.nextElementSibling);
						else container.appendChild(wheelEl);
					} catch (e) {
						console.warn("Unexpected error while rendering the Tracker Preview wheel", e);
					}
				});
			});
			const observer = new MutationObserver((mutations) => {
				if (mutations.some((m) => m.addedNodes.length)) {
					observer.disconnect();
					setTimeout(() => setupTrackersPreview(popupUrl), 500);
				}
			});
			observer.observe(document.body, {
				childList: true,
				subtree: true
			});
		}
	}
	window.addEventListener("message", (message) => {
		if (message.origin + "/" !== chrome.runtime.getURL("/").toLowerCase() || typeof message.data !== "string") return;
		if (message.data === "WTMReportClosePopups") closePopups();
		else if (message.data === "WTMReportDisable") {
			closePopups();
			[...document.querySelectorAll("[data-wtm]")].forEach((el) => {
				delete el.dataset.wtm;
			});
			[...document.querySelectorAll(".wtm-tracker-wheel-container")].forEach((el) => {
				el.parentElement.removeChild(el);
			});
			chrome.runtime.sendMessage({ action: "disableWTMReport" });
		} else if (message.data?.startsWith("WTMReportResize")) {
			const height = message.data.split(":")[1];
			resizePopup(height);
		}
	});
	document.addEventListener("DOMContentLoaded", () => {
		setupTrackersPreview(chrome.runtime.getURL("pages/trackers-preview/index.html"));
	});
	//#endregion
})();
