//#region node_modules/linkedom/esm/shared/register-html-class.js
var htmlClasses = /* @__PURE__ */ new Map();
var registerHTMLClass = (names, Class) => {
	for (const name of [].concat(names)) {
		htmlClasses.set(name, Class);
		htmlClasses.set(name.toUpperCase(), Class);
	}
};
//#endregion
export { htmlClasses, registerHTMLClass };
