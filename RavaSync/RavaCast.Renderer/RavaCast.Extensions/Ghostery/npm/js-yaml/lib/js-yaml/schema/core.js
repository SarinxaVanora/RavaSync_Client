import { __commonJSMin } from "../../../../../virtual/_rolldown/runtime.js";
import { require_schema } from "../schema.js";
import { require_json } from "./json.js";
//#region node_modules/js-yaml/lib/js-yaml/schema/core.js
var require_core = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = new (require_schema())({ include: [require_json()] });
}));
//#endregion
export { require_core };
