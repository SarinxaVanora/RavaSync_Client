import { registerMethods } from "../../utils/methods.js";
import { delimiter } from "../core/regex.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/optional/class.js
function classes() {
	const attr = this.attr("class");
	return attr == null ? [] : attr.trim().split(delimiter);
}
function hasClass(name) {
	return this.classes().indexOf(name) !== -1;
}
function addClass(name) {
	if (!this.hasClass(name)) {
		const array = this.classes();
		array.push(name);
		this.attr("class", array.join(" "));
	}
	return this;
}
function removeClass(name) {
	if (this.hasClass(name)) this.attr("class", this.classes().filter(function(c) {
		return c !== name;
	}).join(" "));
	return this;
}
function toggleClass(name) {
	return this.hasClass(name) ? this.removeClass(name) : this.addClass(name);
}
registerMethods("Dom", {
	classes,
	hasClass,
	addClass,
	removeClass,
	toggleClass
});
//#endregion
