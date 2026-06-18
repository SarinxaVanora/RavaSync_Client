import { MIME } from "../shared/symbols.js";
import { Document } from "../interface/document.js";
//#region node_modules/linkedom/esm/xml/document.js
/**
* @implements globalThis.XMLDocument
*/
var XMLDocument = class extends Document {
	constructor() {
		super("text/xml");
	}
	toString() {
		return this[MIME].docType + super.toString();
	}
};
//#endregion
export { XMLDocument };
