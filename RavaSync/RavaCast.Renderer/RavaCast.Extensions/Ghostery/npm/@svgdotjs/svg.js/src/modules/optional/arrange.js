import { registerMethods } from "../../utils/methods.js";
import { makeInstance } from "../../utils/adopter.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/optional/arrange.js
function siblings() {
	return this.parent().children();
}
function position() {
	return this.parent().index(this);
}
function next() {
	return this.siblings()[this.position() + 1];
}
function prev() {
	return this.siblings()[this.position() - 1];
}
function forward() {
	const i = this.position();
	this.parent().add(this.remove(), i + 1);
	return this;
}
function backward() {
	const i = this.position();
	this.parent().add(this.remove(), i ? i - 1 : 0);
	return this;
}
function front() {
	this.parent().add(this.remove());
	return this;
}
function back() {
	this.parent().add(this.remove(), 0);
	return this;
}
function before(element) {
	element = makeInstance(element);
	element.remove();
	const i = this.position();
	this.parent().add(element, i);
	return this;
}
function after(element) {
	element = makeInstance(element);
	element.remove();
	const i = this.position();
	this.parent().add(element, i + 1);
	return this;
}
function insertBefore(element) {
	element = makeInstance(element);
	element.before(this);
	return this;
}
function insertAfter(element) {
	element = makeInstance(element);
	element.after(this);
	return this;
}
registerMethods("Dom", {
	siblings,
	position,
	next,
	prev,
	forward,
	backward,
	front,
	back,
	before,
	after,
	insertBefore,
	insertAfter
});
//#endregion
