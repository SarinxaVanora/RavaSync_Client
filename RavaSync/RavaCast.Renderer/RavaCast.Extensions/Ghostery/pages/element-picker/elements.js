import define_default from "../../npm/hybrids/src/define.js";
import container_default from "./components/container.js";
import footer_default from "./components/footer.js";
import range_default from "./components/range.js";
//#region src/pages/element-picker/elements.js
define_default.from(/* @__PURE__ */ Object.assign({
	"./components/container.js": container_default,
	"./components/footer.js": footer_default,
	"./components/range.js": range_default
}), {
	root: ["components", "views"],
	prefix: "element-picker"
});
//#endregion
