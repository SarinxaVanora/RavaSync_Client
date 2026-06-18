import { __commonJSMin } from "../../virtual/_rolldown/runtime.js";
import { require_cidr_regex } from "./npm/cidr-regex/index.js";
//#region node_modules/is-cidr/index.js
var require_is_cidr = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var { v4, v6 } = require_cidr_regex();
	var re4 = v4({ exact: true });
	var re6 = v6({ exact: true });
	module.exports = (str) => re4.test(str) ? 4 : re6.test(str) ? 6 : 0;
	module.exports.v4 = (str) => re4.test(str);
	module.exports.v6 = (str) => re6.test(str);
}));
//#endregion
export { require_is_cidr };
