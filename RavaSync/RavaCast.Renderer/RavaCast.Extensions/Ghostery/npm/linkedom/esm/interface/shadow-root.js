import { getInnerHtml, setInnerHtml } from "../mixin/inner-html.js";
import { NonElementParentNode } from "../mixin/non-element-parent-node.js";
//#region node_modules/linkedom/esm/interface/shadow-root.js
/**
* @implements globalThis.ShadowRoot
*/
var ShadowRoot = class extends NonElementParentNode {
	constructor(host) {
		super(host.ownerDocument, "#shadow-root", 11);
		this.host = host;
	}
	get innerHTML() {
		return getInnerHtml(this);
	}
	set innerHTML(html) {
		setInnerHtml(this, html);
	}
};
//#endregion
export { ShadowRoot };
