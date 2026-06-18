import { __commonJSMin } from "../../../virtual/_rolldown/runtime.js";
import { require_exception } from "./js-yaml/exception.js";
import { require_type } from "./js-yaml/type.js";
import { require_schema } from "./js-yaml/schema.js";
import { require_failsafe } from "./js-yaml/schema/failsafe.js";
import { require_json } from "./js-yaml/schema/json.js";
import { require_core } from "./js-yaml/schema/core.js";
import { require_default_safe } from "./js-yaml/schema/default_safe.js";
import { require_default_full } from "./js-yaml/schema/default_full.js";
import { require_loader } from "./js-yaml/loader.js";
import { require_dumper } from "./js-yaml/dumper.js";
//#region node_modules/js-yaml/lib/js-yaml.js
var require_js_yaml = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var loader = require_loader();
	var dumper = require_dumper();
	function deprecated(name) {
		return function() {
			throw new Error("Function " + name + " is deprecated and cannot be used.");
		};
	}
	module.exports.Type = require_type();
	module.exports.Schema = require_schema();
	module.exports.FAILSAFE_SCHEMA = require_failsafe();
	module.exports.JSON_SCHEMA = require_json();
	module.exports.CORE_SCHEMA = require_core();
	module.exports.DEFAULT_SAFE_SCHEMA = require_default_safe();
	module.exports.DEFAULT_FULL_SCHEMA = require_default_full();
	module.exports.load = loader.load;
	module.exports.loadAll = loader.loadAll;
	module.exports.safeLoad = loader.safeLoad;
	module.exports.safeLoadAll = loader.safeLoadAll;
	module.exports.dump = dumper.dump;
	module.exports.safeDump = dumper.safeDump;
	module.exports.YAMLException = require_exception();
	module.exports.MINIMAL_SCHEMA = require_failsafe();
	module.exports.SAFE_SCHEMA = require_default_safe();
	module.exports.DEFAULT_SCHEMA = require_default_full();
	module.exports.scan = deprecated("scan");
	module.exports.parse = deprecated("parse");
	module.exports.compose = deprecated("compose");
	module.exports.addConstructor = deprecated("addConstructor");
}));
//#endregion
export { require_js_yaml };
