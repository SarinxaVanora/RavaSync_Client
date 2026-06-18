import immutable_url_default from "../../../../@ghostery/url-parser/dist/esm/immutable-url.js";
import "../../../../@ghostery/url-parser/dist/esm/index.js";
import logger_default from "./logger.js";
//#region node_modules/@whotracksme/reporting/reporting/src/url-analyzer.js
/**
* WhoTracks.Me
* https://whotracks.me/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
/**
* Can be used to add search engines. Detecting search engines enables
* some optimizations (e.g. quorum checks can be skipped).
*/
var URL_PATTERNS = [
	{
		category: "search-gos",
		regexp: /^https:[/][/]scholar[.]google[.][^/]+[/]scholar.*[?&]q=/,
		prefix: "scholar?q="
	},
	{
		category: "search-goi",
		regexp: /^https:[/][/][^/]*[.]google[.].*?[#?&;]((q=[^&]+&([^&]+&)*udm=2)|(udm=2&([^&]+&)*q=[^&]+))/,
		prefix: "search?udm=2&q="
	},
	{
		category: "search-gov",
		regexp: /^https:[/][/][^/]*[.]google[.].*?[#?&;]((q=[^&]+&([^&]+&)*udm=7)|(udm=7&([^&]+&)*q=[^&]+))/,
		prefix: "search?udm=7&q="
	},
	{
		category: "search-go",
		regexp: /^https:[/][/][^/]*[.]google[.].*?[#?&;]/,
		prefix: "search?q="
	},
	{
		category: "search-ya",
		regexp: /^https:[/][/][^/]*[.]search[.]yahoo[.].*?[#?&;][pq]=[^$&]+/,
		prefix: "search?q=",
		queryFinder(parsedUrl) {
			return parsedUrl.searchParams.get("q") || parsedUrl.searchParams.get("p");
		}
	},
	{
		category: "search-bii",
		regexp: /^https:[/][/][^/]*[.]bing[.][^/]+[/]images[/]search[?]q=[^$&]+/,
		prefix: "images/search?q="
	},
	{
		category: "search-bi",
		regexp: /^https:[/][/][^/]*[.]bing[.].*?[#?&;]q=[^$&]+/,
		prefix: "search?q="
	},
	{
		category: "search-am",
		regexp: /^https:[/][/][^/]*[.]amazon[.][^/]+[/](s[?]k=[^$&]+|.*[?&]field-keywords=[^$&]+)/,
		prefix: "s/?field-keywords=",
		queryFinder(parsedUrl) {
			return parsedUrl.searchParams.get("field-keywords") || parsedUrl.searchParams.get("k");
		}
	},
	{
		category: "search-dd",
		regexp: /^https:[/][/](?:html[.])?duckduckgo[.]com[/].*([?&]q=[^&]+.*&ia=web|[?]q=[^&]+$)/,
		prefix: "html?q=",
		doublefetchHost() {
			return "html.duckduckgo.com";
		}
	},
	{
		category: "search-gh",
		regexp: /^https:[/][/](glowstery|ghosterysearch)[.]com[/]search[?]q=[^&]+/,
		prefix: "search?q="
	},
	{
		category: "search-ghi",
		regexp: /^https:[/][/](glowstery|ghosterysearch)[.]com[/]images[?]q=[^&]+/,
		prefix: "search?q="
	},
	{
		category: "search-ghv",
		regexp: /^https:[/][/](glowstery|ghosterysearch)[.]com[/]videos[?]q=[^&]+/,
		prefix: "search?q="
	},
	{
		category: "search-br",
		regexp: /^https:[/][/]search[.]brave[.]com[/]search[?]q=[^&]+/,
		prefix: "search?q="
	},
	{
		category: "search-bri",
		regexp: /^https:[/][/]search[.]brave[.]com[/]images[?]q=[^&]+/,
		prefix: "images?q="
	},
	{
		category: "search-brn",
		regexp: /^https:[/][/]search[.]brave[.]com[/]news[?]q=[^&]+/,
		prefix: "news?q="
	},
	{
		category: "search-brv",
		regexp: /^https:[/][/]search[.]brave[.]com[/]videos[?]q=[^&]+/,
		prefix: "videos?q="
	},
	{
		category: "search-ec",
		regexp: /^https:[/][/]www[.]ecosia[.]org[/]search[?](?:method=index&)?q=[^&]+/,
		prefix: "search?q="
	}
];
var UrlAnalyzer = class {
	constructor(patterns) {
		this.patterns = patterns;
		this._urlPatterns = URL_PATTERNS;
	}
	parseSearchLinks(url) {
		for (const { category, regexp, prefix, queryFinder = (parsedUrl) => parsedUrl.searchParams.get("q"), doublefetchHost = (parsedUrl) => parsedUrl.host } of this._urlPatterns) if (regexp.test(url)) {
			const parsedUrl = new immutable_url_default(url.replaceAll("+", "%20"));
			const query = queryFinder(parsedUrl);
			if (!query) return { isSupported: false };
			const query_ = encodeURIComponent(query).replaceAll("%20", "+");
			const doublefetchUrl = `https://${doublefetchHost(parsedUrl)}/${prefix}${query_}`;
			const doublefetchRequest = this.patterns.createDoublefetchRequest(category, doublefetchUrl);
			if (!doublefetchRequest) {
				logger_default.debug("Matching rule for", url, "skipped (no matching server side rules exist)");
				return {
					isSupported: false,
					category,
					query
				};
			}
			return {
				isSupported: true,
				category,
				query,
				doublefetchRequest
			};
		}
		return { isSupported: false };
	}
};
//#endregion
export { UrlAnalyzer as default };
