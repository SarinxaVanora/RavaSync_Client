import "./monkey-patch.js";
import convert$1 from "../../npm/@ghostery/urlfilter2dnr/dist/esm/converters/adguard.js";
//#region src/pages/dnr-converter/index.js
async function convert(filters) {
	try {
		return await convert$1(filters, { resourcesPath: "/rule_resources/redirects" });
	} catch (err) {
		console.error("Error converting filters:", err);
		return {
			rules: [],
			errors: [err]
		};
	}
}
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
	if (msg.action === "dnr-converter:convert") {
		convert(msg.filters).then((result) => sendResponse(result), (err) => sendResponse({
			rules: [],
			errors: [err.message]
		}));
		return true;
	}
	return false;
});
//#endregion
