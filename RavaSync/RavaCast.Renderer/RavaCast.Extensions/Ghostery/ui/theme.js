import { addListener } from "../utils/options-observer.js";
import { getLocalStorageItem, setLocalStorageItem } from "../utils/storage.js";
//#region src/ui/theme.js
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
var mode = getLocalStorageItem("theme") || "";
var styleSheets = new Set(Array.from(document.styleSheets));
function updateStylesheet(styleSheet) {
	for (const rule of styleSheet.cssRules) if (rule.media && rule.media.mediaText.includes("prefers-color-scheme")) switch (mode) {
		case "light":
			rule.media.appendMedium("(prefers-color-scheme: original)");
			if (rule.media.mediaText.includes("light")) rule.media.deleteMedium("(prefers-color-scheme: light)");
			if (rule.media.mediaText.includes("dark")) rule.media.deleteMedium("(prefers-color-scheme: dark)");
			break;
		case "dark":
			rule.media.appendMedium("(prefers-color-scheme: light)");
			rule.media.appendMedium("(prefers-color-scheme: dark)");
			if (rule.media.mediaText.includes("original")) rule.media.deleteMedium("(prefers-color-scheme: original)");
			break;
		default:
			if (!rule.media.mediaText.includes("dark")) rule.media.appendMedium("(prefers-color-scheme: dark)");
			if (rule.media.mediaText.includes("light")) rule.media.deleteMedium("(prefers-color-scheme: light)");
			if (rule.media.mediaText.includes("original")) rule.media.deleteMedium("(prefers-color-scheme: original)");
			break;
	}
}
function reloadTheme() {
	for (const sheet of styleSheets) updateStylesheet(sheet);
}
reloadTheme();
if (document.documentElement.dataset.theme) mode = document.documentElement.dataset.theme;
else addListener("theme", (theme, lastTheme) => {
	if (theme || lastTheme) {
		setLocalStorageItem("theme", theme);
		mode = theme;
		reloadTheme();
	}
});
function themeToggle(fn) {
	return (host, target) => {
		const result = fn(host, target);
		if (result.adoptedStyleSheets) {
			for (const sheet of result.adoptedStyleSheets) if (!styleSheets.has(sheet)) {
				styleSheets.add(sheet);
				updateStylesheet(sheet);
			}
		}
		return result;
	};
}
//#endregion
export { themeToggle };
