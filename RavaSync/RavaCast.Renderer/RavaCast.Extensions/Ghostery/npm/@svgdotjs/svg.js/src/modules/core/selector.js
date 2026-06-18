import { map } from "../../utils/utils.js";
import { globals } from "../../utils/window.js";
import { adopt } from "../../utils/adopter.js";
import List from "../../types/List.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/selector.js
function baseFind(query, parent) {
	return new List(map((parent || globals.document).querySelectorAll(query), function(node) {
		return adopt(node);
	}));
}
function find(query) {
	return baseFind(query, this.node);
}
function findOne(query) {
	return adopt(this.node.querySelector(query));
}
//#endregion
export { baseFind as default, find, findOne };
