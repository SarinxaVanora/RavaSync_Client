import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
//#region node_modules/linkedom/commonjs/canvas-shim.cjs
var require_canvas_shim = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var Canvas = class {
		constructor(width, height) {
			this.width = width;
			this.height = height;
		}
		getContext() {
			return null;
		}
		toDataURL() {
			return "";
		}
	};
	module.exports = { createCanvas: (width, height) => new Canvas(width, height) };
}));
//#endregion
export { require_canvas_shim };
