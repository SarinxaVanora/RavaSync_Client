import define_default from "../../npm/hybrids/src/define.js";
import dialog_default from "./components/dialog.js";
import error_card_default from "./components/error-card.js";
import feature_default from "./components/feature.js";
import pin_it_default from "./components/pin-it.js";
import step_default from "./components/step.js";
import addon_health_default from "./views/addon-health.js";
import web_trackers_default from "./views/web-trackers.js";
import performance_default from "./views/performance.js";
import privacy_default from "./views/privacy.js";
import skip_default from "./views/skip.js";
import success_default from "./views/success.js";
import modes_default from "./views/modes.js";
import main_default from "./views/main.js";
//#region src/pages/onboarding/elements.js
define_default.from(/* @__PURE__ */ Object.assign({
	"./components/dialog.js": dialog_default,
	"./components/error-card.js": error_card_default,
	"./components/feature.js": feature_default,
	"./components/pin-it.js": pin_it_default,
	"./components/step.js": step_default,
	"./views/addon-health.js": addon_health_default,
	"./views/main.js": main_default,
	"./views/modes.js": modes_default,
	"./views/performance.js": performance_default,
	"./views/privacy.js": privacy_default,
	"./views/skip.js": skip_default,
	"./views/success.js": success_default,
	"./views/web-trackers.js": web_trackers_default
}), {
	prefix: "onboarding",
	root: "components"
});
//#endregion
