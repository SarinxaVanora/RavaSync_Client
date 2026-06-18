//#region src/utils/regions.js
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
var REGIONS = [
	"ar",
	"bg",
	"cs",
	"de",
	"el",
	"es",
	"fa",
	"fr",
	"he",
	"hi",
	"hu",
	"id",
	"it",
	"ja",
	"ko",
	"lt",
	"lv",
	"nl",
	"pl",
	"pt",
	"ro",
	"ru",
	"sv",
	"tr",
	"vi",
	"zh"
];
var DEFAULT_REGIONS = (navigator.languages || [navigator.language]).map((lang) => lang.split("-")[0].toLowerCase()).filter((lang, i, list) => REGIONS.includes(lang) && list.indexOf(lang) === i);
//#endregion
export { DEFAULT_REGIONS, REGIONS as default };
