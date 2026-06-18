import define_default from "../../npm/hybrids/src/define.js";
import button_default from "./components/button.js";
import sankey_chart_default from "./components/sankey-chart.js";
import trends_chart_default from "./components/trends-chart.js";
//#region src/pages/whotracksme/elements.js
define_default.from(/* @__PURE__ */ Object.assign({
	"./components/button.js": button_default,
	"./components/sankey-chart.js": sankey_chart_default,
	"./components/trends-chart.js": trends_chart_default
}), {
	root: ["components"],
	prefix: "whotracksme"
});
//#endregion
