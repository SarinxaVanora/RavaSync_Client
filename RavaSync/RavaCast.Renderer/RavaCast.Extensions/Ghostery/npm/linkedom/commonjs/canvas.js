import { __commonJSMin, __require } from "../../../virtual/_rolldown/runtime.js";
import { require_canvas_shim } from "./canvas-shim.js";
//#region node_modules/linkedom/commonjs/canvas.cjs
var require_canvas = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	/* c8 ignore start */
	try {
		module.exports = __require("canvas");
	} catch (fallback) {
		module.exports = require_canvas_shim();
	}
}));
/* c8 ignore stop */
//#endregion
export { require_canvas };
