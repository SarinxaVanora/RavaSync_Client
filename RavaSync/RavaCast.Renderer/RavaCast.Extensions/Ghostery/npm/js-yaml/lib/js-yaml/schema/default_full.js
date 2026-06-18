import { __commonJSMin } from "../../../../../virtual/_rolldown/runtime.js";
import { require_schema } from "../schema.js";
import { require_default_safe } from "./default_safe.js";
import { require_undefined } from "../type/js/undefined.js";
import { require_regexp } from "../type/js/regexp.js";
import { require_function } from "../type/js/function.js";
//#region node_modules/js-yaml/lib/js-yaml/schema/default_full.js
var require_default_full = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var Schema = require_schema();
	module.exports = Schema.DEFAULT = new Schema({
		include: [require_default_safe()],
		explicit: [
			require_undefined(),
			require_regexp(),
			require_function()
		]
	});
}));
//#endregion
export { require_default_full };
