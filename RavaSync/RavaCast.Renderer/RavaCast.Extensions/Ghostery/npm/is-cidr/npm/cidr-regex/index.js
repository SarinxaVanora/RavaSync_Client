import { __commonJSMin } from "../../../../virtual/_rolldown/runtime.js";
import { require_ip_regex } from "../ip-regex/index.js";
//#region node_modules/is-cidr/node_modules/cidr-regex/index.js
var require_cidr_regex = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var ipRegex = require_ip_regex();
	var defaultOpts = { exact: false };
	var v4str = `${ipRegex.v4().source}\\/(3[0-2]|[12]?[0-9])`;
	var v6str = `${ipRegex.v6().source}\\/(12[0-8]|1[01][0-9]|[1-9]?[0-9])`;
	var v4exact = new RegExp(`^${v4str}$`);
	var v6exact = new RegExp(`^${v6str}$`);
	var v46exact = new RegExp(`(?:^${v4str}$)|(?:^${v6str}$)`);
	module.exports = ({ exact } = defaultOpts) => exact ? v46exact : new RegExp(`(?:${v4str})|(?:${v6str})`, "g");
	module.exports.v4 = ({ exact } = defaultOpts) => exact ? v4exact : new RegExp(v4str, "g");
	module.exports.v6 = ({ exact } = defaultOpts) => exact ? v6exact : new RegExp(v6str, "g");
}));
//#endregion
export { require_cidr_regex };
