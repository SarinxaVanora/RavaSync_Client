import { __commonJSMin } from "../../../../../virtual/_rolldown/runtime.js";
import { require_schema } from "../schema.js";
import { require_str } from "../type/str.js";
import { require_seq } from "../type/seq.js";
import { require_map } from "../type/map.js";
//#region node_modules/js-yaml/lib/js-yaml/schema/failsafe.js
var require_failsafe = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = new (require_schema())({ explicit: [
		require_str(),
		require_seq(),
		require_map()
	] });
}));
//#endregion
export { require_failsafe };
