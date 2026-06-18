import { PLATFORMS_MAP } from "./constants.js";
import Utils from "./utils.js";
//#region node_modules/bowser/src/parser-platforms.js
var parser_platforms_default = [
	{
		test: [/googlebot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Google"
			};
		}
	},
	{
		test: [/linespider/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Line"
			};
		}
	},
	{
		test: [/amazonbot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Amazon"
			};
		}
	},
	{
		test: [/gptbot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "OpenAI"
			};
		}
	},
	{
		test: [/chatgpt-user/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "OpenAI"
			};
		}
	},
	{
		test: [/oai-searchbot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "OpenAI"
			};
		}
	},
	{
		test: [/baiduspider/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Baidu"
			};
		}
	},
	{
		test: [/bingbot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Bing"
			};
		}
	},
	{
		test: [/duckduckbot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "DuckDuckGo"
			};
		}
	},
	{
		test: [
			/claudebot/i,
			/claude-web/i,
			/claude-user/i,
			/claude-searchbot/i
		],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Anthropic"
			};
		}
	},
	{
		test: [/omgilibot/i, /webzio-extended/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Webz.io"
			};
		}
	},
	{
		test: [/diffbot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Diffbot"
			};
		}
	},
	{
		test: [/perplexitybot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Perplexity AI"
			};
		}
	},
	{
		test: [/perplexity-user/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Perplexity AI"
			};
		}
	},
	{
		test: [/youbot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "You.com"
			};
		}
	},
	{
		test: [/ia_archiver/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Internet Archive"
			};
		}
	},
	{
		test: [/meta-webindexer/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Meta"
			};
		}
	},
	{
		test: [/meta-externalads/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Meta"
			};
		}
	},
	{
		test: [/meta-externalagent/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Meta"
			};
		}
	},
	{
		test: [/meta-externalfetcher/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Meta"
			};
		}
	},
	{
		test: [/facebookexternalhit/i, /facebookcatalog/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Meta"
			};
		}
	},
	{
		test: [/slackbot/i, /slack-imgProxy/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Slack"
			};
		}
	},
	{
		test: [/yahoo/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Yahoo"
			};
		}
	},
	{
		test: [/yandexbot/i, /yandexmobilebot/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Yandex"
			};
		}
	},
	{
		test: [/pingdom/i],
		describe() {
			return {
				type: PLATFORMS_MAP.bot,
				vendor: "Pingdom"
			};
		}
	},
	{
		test: [/huawei/i],
		describe(ua) {
			const model = Utils.getFirstMatch(/(can-l01)/i, ua) && "Nova";
			const platform = {
				type: PLATFORMS_MAP.mobile,
				vendor: "Huawei"
			};
			if (model) platform.model = model;
			return platform;
		}
	},
	{
		test: [/nexus\s*(?:7|8|9|10).*/i],
		describe() {
			return {
				type: PLATFORMS_MAP.tablet,
				vendor: "Nexus"
			};
		}
	},
	{
		test: [/ipad/i],
		describe() {
			return {
				type: PLATFORMS_MAP.tablet,
				vendor: "Apple",
				model: "iPad"
			};
		}
	},
	{
		test: [/Macintosh(.*?) FxiOS(.*?)\//],
		describe() {
			return {
				type: PLATFORMS_MAP.tablet,
				vendor: "Apple",
				model: "iPad"
			};
		}
	},
	{
		test: [/kftt build/i],
		describe() {
			return {
				type: PLATFORMS_MAP.tablet,
				vendor: "Amazon",
				model: "Kindle Fire HD 7"
			};
		}
	},
	{
		test: [/silk/i],
		describe() {
			return {
				type: PLATFORMS_MAP.tablet,
				vendor: "Amazon"
			};
		}
	},
	{
		test: [/tablet(?! pc)/i],
		describe() {
			return { type: PLATFORMS_MAP.tablet };
		}
	},
	{
		test(parser) {
			const iDevice = parser.test(/ipod|iphone/i);
			const likeIDevice = parser.test(/like (ipod|iphone)/i);
			return iDevice && !likeIDevice;
		},
		describe(ua) {
			const model = Utils.getFirstMatch(/(ipod|iphone)/i, ua);
			return {
				type: PLATFORMS_MAP.mobile,
				vendor: "Apple",
				model
			};
		}
	},
	{
		test: [/nexus\s*[0-6].*/i, /galaxy nexus/i],
		describe() {
			return {
				type: PLATFORMS_MAP.mobile,
				vendor: "Nexus"
			};
		}
	},
	{
		test: [/Nokia/i],
		describe(ua) {
			const model = Utils.getFirstMatch(/Nokia\s+([0-9]+(\.[0-9]+)?)/i, ua);
			const platform = {
				type: PLATFORMS_MAP.mobile,
				vendor: "Nokia"
			};
			if (model) platform.model = model;
			return platform;
		}
	},
	{
		test: [/[^-]mobi/i],
		describe() {
			return { type: PLATFORMS_MAP.mobile };
		}
	},
	{
		test(parser) {
			return parser.getBrowserName(true) === "blackberry";
		},
		describe() {
			return {
				type: PLATFORMS_MAP.mobile,
				vendor: "BlackBerry"
			};
		}
	},
	{
		test(parser) {
			return parser.getBrowserName(true) === "bada";
		},
		describe() {
			return { type: PLATFORMS_MAP.mobile };
		}
	},
	{
		test(parser) {
			return parser.getBrowserName() === "windows phone";
		},
		describe() {
			return {
				type: PLATFORMS_MAP.mobile,
				vendor: "Microsoft"
			};
		}
	},
	{
		test(parser) {
			const osMajorVersion = Number(String(parser.getOSVersion()).split(".")[0]);
			return parser.getOSName(true) === "android" && osMajorVersion >= 3;
		},
		describe() {
			return { type: PLATFORMS_MAP.tablet };
		}
	},
	{
		test(parser) {
			return parser.getOSName(true) === "android";
		},
		describe() {
			return { type: PLATFORMS_MAP.mobile };
		}
	},
	{
		test: [/smart-?tv|smarttv/i],
		describe() {
			return { type: PLATFORMS_MAP.tv };
		}
	},
	{
		test: [/netcast/i],
		describe() {
			return { type: PLATFORMS_MAP.tv };
		}
	},
	{
		test(parser) {
			return parser.getOSName(true) === "macos";
		},
		describe() {
			return {
				type: PLATFORMS_MAP.desktop,
				vendor: "Apple"
			};
		}
	},
	{
		test(parser) {
			return parser.getOSName(true) === "windows";
		},
		describe() {
			return { type: PLATFORMS_MAP.desktop };
		}
	},
	{
		test(parser) {
			return parser.getOSName(true) === "linux";
		},
		describe() {
			return { type: PLATFORMS_MAP.desktop };
		}
	},
	{
		test(parser) {
			return parser.getOSName(true) === "playstation 4";
		},
		describe() {
			return { type: PLATFORMS_MAP.tv };
		}
	},
	{
		test(parser) {
			return parser.getOSName(true) === "roku";
		},
		describe() {
			return { type: PLATFORMS_MAP.tv };
		}
	}
];
//#endregion
export { parser_platforms_default as default };
