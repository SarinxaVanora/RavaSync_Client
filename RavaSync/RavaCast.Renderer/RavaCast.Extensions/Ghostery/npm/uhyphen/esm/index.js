//#region node_modules/uhyphen/esm/index.js
var esm_default = (camel) => camel.replace(/(([A-Z0-9])([A-Z0-9][a-z]))|(([a-z0-9]+)([A-Z]))/g, "$2$5-$3$6").toLowerCase();
//#endregion
export { esm_default as default };
