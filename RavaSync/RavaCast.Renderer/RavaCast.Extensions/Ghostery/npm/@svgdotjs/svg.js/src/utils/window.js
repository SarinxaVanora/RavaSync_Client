//#region node_modules/@svgdotjs/svg.js/src/utils/window.js
var globals = {
	window: typeof window === "undefined" ? null : window,
	document: typeof document === "undefined" ? null : document
};
function getWindow() {
	return globals.window;
}
//#endregion
export { getWindow, globals };
