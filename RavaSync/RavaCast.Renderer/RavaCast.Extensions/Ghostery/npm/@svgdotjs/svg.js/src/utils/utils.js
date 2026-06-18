//#region node_modules/@svgdotjs/svg.js/src/utils/utils.js
function map(array, block) {
	let i;
	const il = array.length;
	const result = [];
	for (i = 0; i < il; i++) result.push(block(array[i]));
	return result;
}
function filter(array, block) {
	let i;
	const il = array.length;
	const result = [];
	for (i = 0; i < il; i++) if (block(array[i])) result.push(array[i]);
	return result;
}
function radians(d) {
	return d % 360 * Math.PI / 180;
}
function unCamelCase(s) {
	return s.replace(/([A-Z])/g, function(m, g) {
		return "-" + g.toLowerCase();
	});
}
function capitalize(s) {
	return s.charAt(0).toUpperCase() + s.slice(1);
}
function proportionalSize(element, width, height, box) {
	if (width == null || height == null) {
		box = box || element.bbox();
		if (width == null) width = box.width / box.height * height;
		else if (height == null) height = box.height / box.width * width;
	}
	return {
		width,
		height
	};
}
/**
* This function adds support for string origins.
* It searches for an origin in o.origin o.ox and o.originX.
* This way, origin: {x: 'center', y: 50} can be passed as well as ox: 'center', oy: 50
**/
function getOrigin(o, element) {
	const origin = o.origin;
	let ox = o.ox != null ? o.ox : o.originX != null ? o.originX : "center";
	let oy = o.oy != null ? o.oy : o.originY != null ? o.originY : "center";
	if (origin != null) [ox, oy] = Array.isArray(origin) ? origin : typeof origin === "object" ? [origin.x, origin.y] : [origin, origin];
	const condX = typeof ox === "string";
	const condY = typeof oy === "string";
	if (condX || condY) {
		const { height, width, x, y } = element.bbox();
		if (condX) ox = ox.includes("left") ? x : ox.includes("right") ? x + width : x + width / 2;
		if (condY) oy = oy.includes("top") ? y : oy.includes("bottom") ? y + height : y + height / 2;
	}
	return [ox, oy];
}
var descriptiveElements = new Set([
	"desc",
	"metadata",
	"title"
]);
var isDescriptive = (element) => descriptiveElements.has(element.nodeName);
var writeDataToDom = (element, data, defaults = {}) => {
	const cloned = { ...data };
	for (const key in cloned) if (cloned[key].valueOf() === defaults[key]) delete cloned[key];
	if (Object.keys(cloned).length) element.node.setAttribute("data-svgjs", JSON.stringify(cloned));
	else {
		element.node.removeAttribute("data-svgjs");
		element.node.removeAttribute("svgjs:data");
	}
};
//#endregion
export { capitalize, filter, getOrigin, isDescriptive, map, proportionalSize, radians, unCamelCase, writeDataToDom };
