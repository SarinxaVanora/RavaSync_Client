import { __commonJSMin } from "../../../../../virtual/_rolldown/runtime.js";
import { require_schema } from "../schema.js";
import { require_core } from "./core.js";
import { require_timestamp } from "../type/timestamp.js";
import { require_merge } from "../type/merge.js";
import { require_binary } from "../type/binary.js";
import { require_omap } from "../type/omap.js";
import { require_pairs } from "../type/pairs.js";
import { require_set } from "../type/set.js";
//#region node_modules/js-yaml/lib/js-yaml/schema/default_safe.js
var require_default_safe = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = new (require_schema())({
		include: [require_core()],
		implicit: [require_timestamp(), require_merge()],
		explicit: [
			require_binary(),
			require_omap(),
			require_pairs(),
			require_set()
		]
	});
}));
//#endregion
export { require_default_safe };
