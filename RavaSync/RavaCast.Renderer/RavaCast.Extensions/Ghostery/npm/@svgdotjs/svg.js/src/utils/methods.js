//#region node_modules/@svgdotjs/svg.js/src/utils/methods.js
var methods = {};
var names = [];
function registerMethods(name, m) {
	if (Array.isArray(name)) {
		for (const _name of name) registerMethods(_name, m);
		return;
	}
	if (typeof name === "object") {
		for (const _name in name) registerMethods(_name, name[_name]);
		return;
	}
	addMethodNames(Object.getOwnPropertyNames(m));
	methods[name] = Object.assign(methods[name] || {}, m);
}
function getMethodsFor(name) {
	return methods[name] || {};
}
function getMethodNames() {
	return [...new Set(names)];
}
function addMethodNames(_names) {
	names.push(..._names);
}
//#endregion
export { addMethodNames, getMethodNames, getMethodsFor, registerMethods };
