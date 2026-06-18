import { getCategoryBgColor, getCategoryKey, order } from "./categories.js";
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
function getOffscreenImageData(size, categories, options) {
	let canvas;
	try {
		canvas = new OffscreenCanvas(size, size);
	} catch {
		canvas = document.createElement("canvas");
		canvas.width = size;
		canvas.height = size;
	}
	const ctx = canvas.getContext("2d");
	drawWheel(ctx, size, categories, {
		useScale: false,
		...options
	});
	return ctx.getImageData(0, 0, size, size);
}
//#endregion
export { getOffscreenImageData };
