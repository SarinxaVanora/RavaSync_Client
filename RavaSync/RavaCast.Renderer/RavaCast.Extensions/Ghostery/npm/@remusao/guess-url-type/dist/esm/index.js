import { EXTENSIONS } from "./extensions/documents.js";
import { EXTENSIONS as EXTENSIONS$1 } from "./extensions/fonts.js";
import { EXTENSIONS as EXTENSIONS$2 } from "./extensions/images.js";
import { EXTENSIONS as EXTENSIONS$3 } from "./extensions/medias.js";
import { EXTENSIONS as EXTENSIONS$4 } from "./extensions/scripts.js";
import { EXTENSIONS as EXTENSIONS$5 } from "./extensions/stylesheets.js";
import { extname } from "./extname.js";
//#region node_modules/@remusao/guess-url-type/dist/esm/index.js
function getRequestType(url) {
	const ext = extname(url);
	if (EXTENSIONS$2.has(ext) || url.startsWith("data:image/") || url.startsWith("https://frog.wix.com/bt")) return "image";
	if (EXTENSIONS$3.has(ext) || url.startsWith("data:audio/") || url.startsWith("data:video/")) return "media";
	if (EXTENSIONS$5.has(ext) || url.startsWith("data:text/css")) return "stylesheet";
	if (EXTENSIONS$4.has(ext) || url.startsWith("data:") && (url.startsWith("data:application/ecmascript") || url.startsWith("data:application/javascript") || url.startsWith("data:application/x-ecmascript") || url.startsWith("data:application/x-javascript") || url.startsWith("data:text/ecmascript") || url.startsWith("data:text/javascript") || url.startsWith("data:text/javascript1.0") || url.startsWith("data:text/javascript1.1") || url.startsWith("data:text/javascript1.2") || url.startsWith("data:text/javascript1.3") || url.startsWith("data:text/javascript1.4") || url.startsWith("data:text/javascript1.5") || url.startsWith("data:text/jscript") || url.startsWith("data:text/livescript") || url.startsWith("data:text/x-ecmascript") || url.startsWith("data:text/x-javascript")) || url.startsWith("https://maps.googleapis.com/maps/api/js") || url.startsWith("https://www.googletagmanager.com/gtag/js")) return "script";
	if (EXTENSIONS.has(ext) || url.startsWith("data:text/html") || url.startsWith("data:application/xhtml") || url.startsWith("https://www.youtube.com/embed/") || url.startsWith("https://www.google.com/gen_204")) return "document";
	if (EXTENSIONS$1.has(ext) || url.startsWith("data:font/")) return "font";
	return "other";
}
//#endregion
export { getRequestType as default };
