import { __commonJSMin } from "../../../../../virtual/_rolldown/runtime.js";
import { require_type } from "../type.js";
//#region node_modules/js-yaml/lib/js-yaml/type/str.js
var require_str = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = new (require_type())("tag:yaml.org,2002:str", {
		kind: "scalar",
		construct: function(data) {
			return data !== null ? data : "";
		}
	});
}));
//#endregion
export { require_str };
