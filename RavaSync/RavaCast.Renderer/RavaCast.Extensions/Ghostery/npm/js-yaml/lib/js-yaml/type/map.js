import { __commonJSMin } from "../../../../../virtual/_rolldown/runtime.js";
import { require_type } from "../type.js";
//#region node_modules/js-yaml/lib/js-yaml/type/map.js
var require_map = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	module.exports = new (require_type())("tag:yaml.org,2002:map", {
		kind: "mapping",
		construct: function(data) {
			return data !== null ? data : {};
		}
	});
}));
//#endregion
export { require_map };
