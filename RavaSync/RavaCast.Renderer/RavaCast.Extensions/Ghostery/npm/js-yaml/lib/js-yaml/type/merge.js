import { __commonJSMin } from "../../../../../virtual/_rolldown/runtime.js";
import { require_type } from "../type.js";
//#region node_modules/js-yaml/lib/js-yaml/type/merge.js
var require_merge = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var Type = require_type();
	function resolveYamlMerge(data) {
		return data === "<<" || data === null;
	}
	module.exports = new Type("tag:yaml.org,2002:merge", {
		kind: "scalar",
		resolve: resolveYamlMerge
	});
}));
//#endregion
export { require_merge };
