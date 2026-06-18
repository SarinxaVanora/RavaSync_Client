import { registerMethods } from "../../utils/methods.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/optional/memory.js
function remember(k, v) {
	if (typeof arguments[0] === "object") for (const key in k) this.remember(key, k[key]);
	else if (arguments.length === 1) return this.memory()[k];
	else this.memory()[k] = v;
	return this;
}
function forget() {
	if (arguments.length === 0) this._memory = {};
	else for (let i = arguments.length - 1; i >= 0; i--) delete this.memory()[arguments[i]];
	return this;
}
function memory() {
	return this._memory = this._memory || {};
}
registerMethods("Dom", {
	remember,
	forget,
	memory
});
//#endregion
