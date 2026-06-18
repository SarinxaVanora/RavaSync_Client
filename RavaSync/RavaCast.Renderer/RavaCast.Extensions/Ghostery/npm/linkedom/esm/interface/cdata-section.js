import { VALUE } from "../shared/symbols.js";
import { CharacterData } from "./character-data.js";
//#region node_modules/linkedom/esm/interface/cdata-section.js
/**
* @implements globalThis.CDATASection
*/
var CDATASection = class CDATASection extends CharacterData {
	constructor(ownerDocument, data = "") {
		super(ownerDocument, "#cdatasection", 4, data);
	}
	cloneNode() {
		const { ownerDocument, [VALUE]: data } = this;
		return new CDATASection(ownerDocument, data);
	}
	toString() {
		return `<![CDATA[${this[VALUE]}]]>`;
	}
};
//#endregion
export { CDATASection };
