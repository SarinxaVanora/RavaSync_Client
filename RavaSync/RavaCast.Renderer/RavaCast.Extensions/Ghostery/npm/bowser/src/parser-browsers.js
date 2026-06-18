import Utils from "./utils.js";
//#region node_modules/bowser/src/parser-browsers.js
/**
* Browsers' descriptors
*
* The idea of descriptors is simple. You should know about them two simple things:
* 1. Every descriptor has a method or property called `test` and a `describe` method.
* 2. Order of descriptors is important.
*
* More details:
* 1. Method or property `test` serves as a way to detect whether the UA string
* matches some certain browser or not. The `describe` method helps to make a result
* object with params that show some browser-specific things: name, version, etc.
* 2. Order of descriptors is important because a Parser goes through them one by one
* in course. For example, if you insert Chrome's descriptor as the first one,
* more then a half of browsers will be described as Chrome, because they will pass
* the Chrome descriptor's test.
*
* Descriptor's `test` could be a property with an array of RegExps, where every RegExp
* will be applied to a UA string to test it whether it matches or not.
* If a descriptor has two or more regexps in the `test` array it tests them one by one
* with a logical sum operation. Parser stops if it has found any RegExp that matches the UA.
*
* Or `test` could be a method. In that case it gets a Parser instance and should
* return true/false to get the Parser know if this browser descriptor matches the UA or not.
*/
var commonVersionIdentifier = /version\/(\d+(\.?_?\d+)+)/i;
var browsersList = [
	{
		test: [/gptbot/i],
		describe(ua) {
			const browser = { name: "GPTBot" };
			const version = Utils.getFirstMatch(/gptbot\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/chatgpt-user/i],
		describe(ua) {
			const browser = { name: "ChatGPT-User" };
			const version = Utils.getFirstMatch(/chatgpt-user\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/oai-searchbot/i],
		describe(ua) {
			const browser = { name: "OAI-SearchBot" };
			const version = Utils.getFirstMatch(/oai-searchbot\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [
			/claudebot/i,
			/claude-web/i,
			/claude-user/i,
			/claude-searchbot/i
		],
		describe(ua) {
			const browser = { name: "ClaudeBot" };
			const version = Utils.getFirstMatch(/(?:claudebot|claude-web|claude-user|claude-searchbot)\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/omgilibot/i, /webzio-extended/i],
		describe(ua) {
			const browser = { name: "Omgilibot" };
			const version = Utils.getFirstMatch(/(?:omgilibot|webzio-extended)\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/diffbot/i],
		describe(ua) {
			const browser = { name: "Diffbot" };
			const version = Utils.getFirstMatch(/diffbot\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/perplexitybot/i],
		describe(ua) {
			const browser = { name: "PerplexityBot" };
			const version = Utils.getFirstMatch(/perplexitybot\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/perplexity-user/i],
		describe(ua) {
			const browser = { name: "Perplexity-User" };
			const version = Utils.getFirstMatch(/perplexity-user\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/youbot/i],
		describe(ua) {
			const browser = { name: "YouBot" };
			const version = Utils.getFirstMatch(/youbot\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/meta-webindexer/i],
		describe(ua) {
			const browser = { name: "Meta-WebIndexer" };
			const version = Utils.getFirstMatch(/meta-webindexer\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/meta-externalads/i],
		describe(ua) {
			const browser = { name: "Meta-ExternalAds" };
			const version = Utils.getFirstMatch(/meta-externalads\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/meta-externalagent/i],
		describe(ua) {
			const browser = { name: "Meta-ExternalAgent" };
			const version = Utils.getFirstMatch(/meta-externalagent\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/meta-externalfetcher/i],
		describe(ua) {
			const browser = { name: "Meta-ExternalFetcher" };
			const version = Utils.getFirstMatch(/meta-externalfetcher\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/googlebot/i],
		describe(ua) {
			const browser = { name: "Googlebot" };
			const version = Utils.getFirstMatch(/googlebot\/(\d+(\.\d+))/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/linespider/i],
		describe(ua) {
			const browser = { name: "Linespider" };
			const version = Utils.getFirstMatch(/(?:linespider)(?:-[-\w]+)?[\s/](\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/amazonbot/i],
		describe(ua) {
			const browser = { name: "AmazonBot" };
			const version = Utils.getFirstMatch(/amazonbot\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/bingbot/i],
		describe(ua) {
			const browser = { name: "BingCrawler" };
			const version = Utils.getFirstMatch(/bingbot\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/baiduspider/i],
		describe(ua) {
			const browser = { name: "BaiduSpider" };
			const version = Utils.getFirstMatch(/baiduspider\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/duckduckbot/i],
		describe(ua) {
			const browser = { name: "DuckDuckBot" };
			const version = Utils.getFirstMatch(/duckduckbot\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/ia_archiver/i],
		describe(ua) {
			const browser = { name: "InternetArchiveCrawler" };
			const version = Utils.getFirstMatch(/ia_archiver\/(\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/facebookexternalhit/i, /facebookcatalog/i],
		describe() {
			return { name: "FacebookExternalHit" };
		}
	},
	{
		test: [/slackbot/i, /slack-imgProxy/i],
		describe(ua) {
			const browser = { name: "SlackBot" };
			const version = Utils.getFirstMatch(/(?:slackbot|slack-imgproxy)(?:-[-\w]+)?[\s/](\d+(\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/yahoo!?[\s/]*slurp/i],
		describe() {
			return { name: "YahooSlurp" };
		}
	},
	{
		test: [/yandexbot/i, /yandexmobilebot/i],
		describe() {
			return { name: "YandexBot" };
		}
	},
	{
		test: [/pingdom/i],
		describe() {
			return { name: "PingdomBot" };
		}
	},
	{
		test: [/opera/i],
		describe(ua) {
			const browser = { name: "Opera" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:opera)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/opr\/|opios/i],
		describe(ua) {
			const browser = { name: "Opera" };
			const version = Utils.getFirstMatch(/(?:opr|opios)[\s/](\S+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/SamsungBrowser/i],
		describe(ua) {
			const browser = { name: "Samsung Internet for Android" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:SamsungBrowser)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/Whale/i],
		describe(ua) {
			const browser = { name: "NAVER Whale Browser" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:whale)[\s/](\d+(?:\.\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/PaleMoon/i],
		describe(ua) {
			const browser = { name: "Pale Moon" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:PaleMoon)[\s/](\d+(?:\.\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/MZBrowser/i],
		describe(ua) {
			const browser = { name: "MZ Browser" };
			const version = Utils.getFirstMatch(/(?:MZBrowser)[\s/](\d+(?:\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/focus/i],
		describe(ua) {
			const browser = { name: "Focus" };
			const version = Utils.getFirstMatch(/(?:focus)[\s/](\d+(?:\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/swing/i],
		describe(ua) {
			const browser = { name: "Swing" };
			const version = Utils.getFirstMatch(/(?:swing)[\s/](\d+(?:\.\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/coast/i],
		describe(ua) {
			const browser = { name: "Opera Coast" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:coast)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/opt\/\d+(?:.?_?\d+)+/i],
		describe(ua) {
			const browser = { name: "Opera Touch" };
			const version = Utils.getFirstMatch(/(?:opt)[\s/](\d+(\.?_?\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/yabrowser/i],
		describe(ua) {
			const browser = { name: "Yandex Browser" };
			const version = Utils.getFirstMatch(/(?:yabrowser)[\s/](\d+(\.?_?\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/ucbrowser/i],
		describe(ua) {
			const browser = { name: "UC Browser" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:ucbrowser)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/Maxthon|mxios/i],
		describe(ua) {
			const browser = { name: "Maxthon" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:Maxthon|mxios)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/epiphany/i],
		describe(ua) {
			const browser = { name: "Epiphany" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:epiphany)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/puffin/i],
		describe(ua) {
			const browser = { name: "Puffin" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:puffin)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/sleipnir/i],
		describe(ua) {
			const browser = { name: "Sleipnir" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:sleipnir)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/k-meleon/i],
		describe(ua) {
			const browser = { name: "K-Meleon" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/(?:k-meleon)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/micromessenger/i],
		describe(ua) {
			const browser = { name: "WeChat" };
			const version = Utils.getFirstMatch(/(?:micromessenger)[\s/](\d+(\.?_?\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/qqbrowser/i],
		describe(ua) {
			const browser = { name: /qqbrowserlite/i.test(ua) ? "QQ Browser Lite" : "QQ Browser" };
			const version = Utils.getFirstMatch(/(?:qqbrowserlite|qqbrowser)[/](\d+(\.?_?\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/msie|trident/i],
		describe(ua) {
			const browser = { name: "Internet Explorer" };
			const version = Utils.getFirstMatch(/(?:msie |rv:)(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/\sedg\//i],
		describe(ua) {
			const browser = { name: "Microsoft Edge" };
			const version = Utils.getFirstMatch(/\sedg\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/edg([ea]|ios)/i],
		describe(ua) {
			const browser = { name: "Microsoft Edge" };
			const version = Utils.getSecondMatch(/edg([ea]|ios)\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/vivaldi/i],
		describe(ua) {
			const browser = { name: "Vivaldi" };
			const version = Utils.getFirstMatch(/vivaldi\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/seamonkey/i],
		describe(ua) {
			const browser = { name: "SeaMonkey" };
			const version = Utils.getFirstMatch(/seamonkey\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/sailfish/i],
		describe(ua) {
			const browser = { name: "Sailfish" };
			const version = Utils.getFirstMatch(/sailfish\s?browser\/(\d+(\.\d+)?)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/silk/i],
		describe(ua) {
			const browser = { name: "Amazon Silk" };
			const version = Utils.getFirstMatch(/silk\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/phantom/i],
		describe(ua) {
			const browser = { name: "PhantomJS" };
			const version = Utils.getFirstMatch(/phantomjs\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/slimerjs/i],
		describe(ua) {
			const browser = { name: "SlimerJS" };
			const version = Utils.getFirstMatch(/slimerjs\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/blackberry|\bbb\d+/i, /rim\stablet/i],
		describe(ua) {
			const browser = { name: "BlackBerry" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/blackberry[\d]+\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/(web|hpw)[o0]s/i],
		describe(ua) {
			const browser = { name: "WebOS Browser" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua) || Utils.getFirstMatch(/w(?:eb)?[o0]sbrowser\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/bada/i],
		describe(ua) {
			const browser = { name: "Bada" };
			const version = Utils.getFirstMatch(/dolfin\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/tizen/i],
		describe(ua) {
			const browser = { name: "Tizen" };
			const version = Utils.getFirstMatch(/(?:tizen\s?)?browser\/(\d+(\.?_?\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/qupzilla/i],
		describe(ua) {
			const browser = { name: "QupZilla" };
			const version = Utils.getFirstMatch(/(?:qupzilla)[\s/](\d+(\.?_?\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/librewolf/i],
		describe(ua) {
			const browser = { name: "LibreWolf" };
			const version = Utils.getFirstMatch(/(?:librewolf)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/firefox|iceweasel|fxios/i],
		describe(ua) {
			const browser = { name: "Firefox" };
			const version = Utils.getFirstMatch(/(?:firefox|iceweasel|fxios)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/electron/i],
		describe(ua) {
			const browser = { name: "Electron" };
			const version = Utils.getFirstMatch(/(?:electron)\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [
			/sogoumobilebrowser/i,
			/metasr/i,
			/se 2\.[x]/i
		],
		describe(ua) {
			const browser = { name: "Sogou Browser" };
			const sogouMobileVersion = Utils.getFirstMatch(/(?:sogoumobilebrowser)[\s/](\d+(\.?_?\d+)+)/i, ua);
			const chromiumVersion = Utils.getFirstMatch(/(?:chrome|crios|crmo)\/(\d+(\.?_?\d+)+)/i, ua);
			const seVersion = Utils.getFirstMatch(/se ([\d.]+)x/i, ua);
			const version = sogouMobileVersion || chromiumVersion || seVersion;
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/MiuiBrowser/i],
		describe(ua) {
			const browser = { name: "Miui" };
			const version = Utils.getFirstMatch(/(?:MiuiBrowser)[\s/](\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test(parser) {
			if (parser.hasBrand("DuckDuckGo")) return true;
			return parser.test(/\sDdg\/[\d.]+$/i);
		},
		describe(ua, parser) {
			const browser = { name: "DuckDuckGo" };
			if (parser) {
				const hintsVersion = parser.getBrandVersion("DuckDuckGo");
				if (hintsVersion) {
					browser.version = hintsVersion;
					return browser;
				}
			}
			const uaVersion = Utils.getFirstMatch(/\sDdg\/([\d.]+)$/i, ua);
			if (uaVersion) browser.version = uaVersion;
			return browser;
		}
	},
	{
		test(parser) {
			return parser.hasBrand("Brave");
		},
		describe(ua, parser) {
			const browser = { name: "Brave" };
			if (parser) {
				const hintsVersion = parser.getBrandVersion("Brave");
				if (hintsVersion) {
					browser.version = hintsVersion;
					return browser;
				}
			}
			return browser;
		}
	},
	{
		test: [/chromium/i],
		describe(ua) {
			const browser = { name: "Chromium" };
			const version = Utils.getFirstMatch(/(?:chromium)[\s/](\d+(\.?_?\d+)+)/i, ua) || Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/chrome|crios|crmo/i],
		describe(ua) {
			const browser = { name: "Chrome" };
			const version = Utils.getFirstMatch(/(?:chrome|crios|crmo)\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/GSA/i],
		describe(ua) {
			const browser = { name: "Google Search" };
			const version = Utils.getFirstMatch(/(?:GSA)\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test(parser) {
			const notLikeAndroid = !parser.test(/like android/i);
			const butAndroid = parser.test(/android/i);
			return notLikeAndroid && butAndroid;
		},
		describe(ua) {
			const browser = { name: "Android Browser" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/playstation 4/i],
		describe(ua) {
			const browser = { name: "PlayStation 4" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/safari|applewebkit/i],
		describe(ua) {
			const browser = { name: "Safari" };
			const version = Utils.getFirstMatch(commonVersionIdentifier, ua);
			if (version) browser.version = version;
			return browser;
		}
	},
	{
		test: [/.*/i],
		describe(ua) {
			const regexp = ua.search("\\(") !== -1 ? /^(.*)\/(.*)[ \t]\((.*)/ : /^(.*)\/(.*) /;
			return {
				name: Utils.getFirstMatch(regexp, ua),
				version: Utils.getSecondMatch(regexp, ua)
			};
		}
	}
];
//#endregion
export { browsersList as default };
