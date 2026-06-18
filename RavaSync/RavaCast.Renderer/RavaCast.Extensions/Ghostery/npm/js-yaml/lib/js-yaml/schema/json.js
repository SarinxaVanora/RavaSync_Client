import { __commonJSMin } from "../../../../../virtual/_rolldown/runtime.js";
import { require_schema } from "../schema.js";
import { require_failsafe } from "./failsafe.js";
import { require_null } from "../type/null.js";
import { require_bool } from "../type/bool.js";
import { require_int } from "../type/int.js";
import { require_float } from "../type/float.js";
//#region node_modules/js-yaml/lib/js-yaml/schema/json.js
var require_json = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = new (require_schema())({
		include: [require_failsafe()],
		implicit: [
			require_null(),
			require_bool(),
			require_int(),
			require_float()
		]
	});
}));
//#endregion
export { require_json };
