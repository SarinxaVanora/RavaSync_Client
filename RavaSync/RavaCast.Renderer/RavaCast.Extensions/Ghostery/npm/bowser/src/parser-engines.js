import { ENGINE_MAP } from "./constants.js";
import Utils from "./utils.js";
//#region node_modules/bowser/src/parser-engines.js
var parser_engines_default = [
	{
		test(parser) {
			return parser.getBrowserName(true) === "microsoft edge";
		},
		describe(ua) {
			if (/\sedg\//i.test(ua)) return { name: ENGINE_MAP.Blink };
			const version = Utils.getFirstMatch(/edge\/(\d+(\.?_?\d+)+)/i, ua);
			return {
				name: ENGINE_MAP.EdgeHTML,
				version
			};
		}
	},
	{
		test: [/trident/i],
		describe(ua) {
			const engine = { name: ENGINE_MAP.Trident };
			const version = Utils.getFirstMatch(/trident\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) engine.version = version;
			return engine;
		}
	},
	{
		test(parser) {
			return parser.test(/presto/i);
		},
		describe(ua) {
			const engine = { name: ENGINE_MAP.Presto };
			const version = Utils.getFirstMatch(/presto\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) engine.version = version;
			return engine;
		}
	},
	{
		test(parser) {
			const isGecko = parser.test(/gecko/i);
			const likeGecko = parser.test(/like gecko/i);
			return isGecko && !likeGecko;
		},
		describe(ua) {
			const engine = { name: ENGINE_MAP.Gecko };
			const version = Utils.getFirstMatch(/gecko\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) engine.version = version;
			return engine;
		}
	},
	{
		test: [/(apple)?webkit\/537\.36/i],
		describe() {
			return { name: ENGINE_MAP.Blink };
		}
	},
	{
		test: [/(apple)?webkit/i],
		describe(ua) {
			const engine = { name: ENGINE_MAP.WebKit };
			const version = Utils.getFirstMatch(/webkit\/(\d+(\.?_?\d+)+)/i, ua);
			if (version) engine.version = version;
			return engine;
		}
	}
];
//#endregion
export { parser_engines_default as default };
