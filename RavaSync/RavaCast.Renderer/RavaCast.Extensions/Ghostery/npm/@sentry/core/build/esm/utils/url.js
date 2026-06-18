//#region node_modules/@sentry/core/build/esm/utils/url.js
/**
* Parses string form of URL into an object
* // borrowed from https://tools.ietf.org/html/rfc3986#appendix-B
* // intentionally using regex and not <a/> href parsing trick because React Native and other
* // environments where DOM might not be available
* @returns parsed URL object
*/
function parseUrl(url) {
	if (!url) return {};
	const match = url.match(/^(([^:/?#]+):)?(\/\/([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?$/);
	if (!match) return {};
	const query = match[6] || "";
	const fragment = match[8] || "";
	return {
		host: match[4],
		path: match[5],
		protocol: match[2],
		search: query,
		hash: fragment,
		relative: match[5] + query + fragment
	};
}
/**
* Strips the content from a data URL, returning a placeholder with the MIME type.
*
* Data URLs can be very long (e.g. base64 encoded scripts for Web Workers),
* with little valuable information, often leading to envelopes getting dropped due
* to size limit violations. Therefore, we strip data URLs and replace them with a
* placeholder.
*
* @param url - The URL to process
* @param includeDataPrefix - If true, includes the first 10 characters of the data stream
*                            for debugging (e.g., to identify magic bytes like WASM's AGFzbQ).
*                            Defaults to true.
* @returns For data URLs, returns a short format like `data:text/javascript;base64,SGVsbG8gV2... [truncated]`.
*          For non-data URLs, returns the original URL unchanged.
*/
function stripDataUrlContent(url, includeDataPrefix = true) {
	if (url.startsWith("data:")) {
		const match = url.match(/^data:([^;,]+)/);
		const mimeType = match ? match[1] : "text/plain";
		const isBase64 = url.includes(";base64,");
		const dataStart = url.indexOf(",");
		let dataPrefix = "";
		if (includeDataPrefix && dataStart !== -1) {
			const data = url.slice(dataStart + 1);
			dataPrefix = data.length > 10 ? `${data.slice(0, 10)}... [truncated]` : data;
		}
		return `data:${mimeType}${isBase64 ? ",base64" : ""}${dataPrefix ? `,${dataPrefix}` : ""}`;
	}
	return url;
}
//#endregion
export { parseUrl, stripDataUrlContent };
