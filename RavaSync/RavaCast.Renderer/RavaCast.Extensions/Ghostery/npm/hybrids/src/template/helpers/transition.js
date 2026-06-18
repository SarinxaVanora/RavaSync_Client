import { stringifyElement } from "../../utils.js";
//#region node_modules/hybrids/src/template/helpers/transition.js
var transition_default = globalThis.document && globalThis.document.startViewTransition !== void 0 && function transition(template) {
	return async function fn(host, target) {
		template.useLayout = fn.useLayout;
		if (transition.instance) {
			console.warn(`${stringifyElement(host)}: view transition already in progress`);
			transition.instance.finished.finally(() => {
				template(host, target);
			});
			return;
		}
		transition.instance = globalThis.document.startViewTransition(() => {
			template(host, target);
		});
		transition.instance.finished.finally(() => {
			transition.instance = void 0;
		});
	};
} || ((fn) => fn);
//#endregion
export { transition_default as default };
