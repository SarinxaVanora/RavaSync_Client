import { __commonJSMin } from "../../virtual/_rolldown/runtime.js";
import { require_ip_regex } from "./npm/ip-regex/index.js";
//#region node_modules/is-ip/index.js
var require_is_ip = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var ipRegex = require_ip_regex();
	var isIp = (string) => ipRegex({ exact: true }).test(string);
	isIp.v4 = (string) => ipRegex.v4({ exact: true }).test(string);
	isIp.v6 = (string) => ipRegex.v6({ exact: true }).test(string);
	isIp.version = (string) => isIp(string) ? isIp.v4(string) ? 4 : 6 : void 0;
	module.exports = isIp;
}));
//#endregion
export { require_is_ip };
