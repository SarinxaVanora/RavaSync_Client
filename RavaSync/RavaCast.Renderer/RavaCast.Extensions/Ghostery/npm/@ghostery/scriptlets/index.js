import scriptlets$1 from "./ubo.js";
//#region node_modules/@ghostery/scriptlets/index.js
var scriptlets = {};
for (const [name, scriptlet] of Object.entries(scriptlets$1)) {
	scriptlets[name] = scriptlet;
	for (const alias of scriptlet.aliases) scriptlets[alias] = scriptlet;
}
//#endregion
export { scriptlets as default };
