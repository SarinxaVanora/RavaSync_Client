import feedback_default from "./feedback.js";
import hands_default from "./hands.js";
import no_websites_default from "./no_websites.js";
import pause_assistant_default from "./pause_assistant.js";
import shield_default from "./shield.js";
import trackers_count_default from "./trackers_count.js";
import trackers_preview_default from "./trackers_preview.js";
import wtm_privacy_report_default from "./wtm_privacy_report.js";
import wtm_wheel_default from "./wtm_wheel.js";
//#region src/pages/settings/assets/index.js
var assets_default = Object.fromEntries(Object.entries(/* @__PURE__ */ Object.assign({
	"./feedback.svg": feedback_default,
	"./hands.svg": hands_default,
	"./no_websites.svg": no_websites_default,
	"./pause_assistant.svg": pause_assistant_default,
	"./shield.svg": shield_default,
	"./trackers_count.svg": trackers_count_default,
	"./trackers_preview.svg": trackers_preview_default,
	"./wtm_privacy_report.svg": wtm_privacy_report_default,
	"./wtm_wheel.svg": wtm_wheel_default
})).map(([path, url]) => [path.replace("./", "").replace(".svg", ""), url]));
//#endregion
export { assets_default as default };
