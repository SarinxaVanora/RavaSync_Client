//#region node_modules/linkedom/esm/shared/mime.js
var voidElements = { test: () => true };
var Mime = {
	"text/html": {
		docType: "<!DOCTYPE html>",
		ignoreCase: true,
		voidElements: /^(?:area|base|br|col|embed|hr|img|input|keygen|link|menuitem|meta|param|source|track|wbr)$/i
	},
	"image/svg+xml": {
		docType: "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
		ignoreCase: false,
		voidElements
	},
	"text/xml": {
		docType: "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
		ignoreCase: false,
		voidElements
	},
	"application/xml": {
		docType: "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
		ignoreCase: false,
		voidElements
	},
	"application/xhtml+xml": {
		docType: "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
		ignoreCase: false,
		voidElements
	}
};
//#endregion
export { Mime };
