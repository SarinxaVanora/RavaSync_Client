import { documentTypeAsJSON } from "../shared/jsdon.js";
import { Node } from "./node.js";
//#region node_modules/linkedom/esm/interface/document-type.js
/**
* @implements globalThis.DocumentType
*/
var DocumentType = class DocumentType extends Node {
	constructor(ownerDocument, name, publicId = "", systemId = "") {
		super(ownerDocument, "#document-type", 10);
		this.name = name;
		this.publicId = publicId;
		this.systemId = systemId;
	}
	cloneNode() {
		const { ownerDocument, name, publicId, systemId } = this;
		return new DocumentType(ownerDocument, name, publicId, systemId);
	}
	toString() {
		const { name, publicId, systemId } = this;
		const hasPublic = 0 < publicId.length;
		const str = [name];
		if (hasPublic) str.push("PUBLIC", `"${publicId}"`);
		if (systemId.length) {
			if (!hasPublic) str.push("SYSTEM");
			str.push(`"${systemId}"`);
		}
		return `<!DOCTYPE ${str.join(" ")}>`;
	}
	toJSON() {
		const json = [];
		documentTypeAsJSON(this, json);
		return json;
	}
};
//#endregion
export { DocumentType };
