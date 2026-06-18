import { parse } from "../../../../tldts-experimental/dist/es6/index.js";
import SearchParams, { extractParams } from "./url-search-params.js";
//#region node_modules/@ghostery/url-parser/dist/esm/immutable-url.js
var BREAK_HOST_ON = [
	47,
	35,
	63
];
function isValidProtocolChar(code) {
	return code >= 65 && code <= 90 || code >= 97 && code <= 122 || code >= 48 && code <= 57 || code === 45 || code === 43;
}
/**
* A Fast implementation of url parsing, mostly API-compatible with the standard URL class while
* being on average 2-3 times faster. Evaluation of URL components is lazy, so this implementation
* should be fast for all use-cases.
*
* Known differences to standard URL:
*  * Parameters returned via `URL.searchParams.entries()` are decoded only with
*    `decodeURIComponent`. This differs to standards parsing in some subtle ways.
*  * You can iterate a URL parameters array directly via `URL.searchParams.params`. This is around
*    20% faster than using an iterator.
*  * Parameter strings are parsed, and accessible via `URL.parameters`.
*  * Domain parsing with tldts is built in. The `URL.domainInfo` attribute returns output from tldts'
*    `parseHost` method.
*  * Hostname validation is not done on initial parse. The `isValidHost()` method is provided for
*    this purpose.
*  * Some extra helper methods.
*
* See also for common API: https://developer.mozilla.org/en-US/docs/Web/API/URL
*/
var immutable_url_default = class {
	constructor(url) {
		this.parse(url);
	}
	get protocol() {
		return this._protocol;
	}
	get username() {
		return this._username;
	}
	get password() {
		return this._password;
	}
	get hostname() {
		return this._hostname;
	}
	get host() {
		return this._host;
	}
	get port() {
		return this._port;
	}
	get pathname() {
		return this._pathname;
	}
	/**
	* The query string component of the URL, including the preceding `?` character.
	* See also: https://developer.mozilla.org/en-US/docs/Web/API/URL/search
	*/
	get search() {
		if (!this._search) this._extractParams();
		return this._search;
	}
	/**
	* Parsed query string parameters, as a `URLSearchParams` object.
	* See also: https://developer.mozilla.org/en-US/docs/Web/API/URL/searchParams
	*/
	get searchParams() {
		if (!this.isQueryParsed) this._extractSearchParams();
		return this._query;
	}
	/**
	* Parsed parameter string from the url. These are `;` separated key/values appearing in the URL
	* path, before the query string.
	*/
	get parameters() {
		if (!this.isQueryParsed) this._extractSearchParams();
		return this._parameters;
	}
	/**
	* Check if the URL has a parameter string
	* @returns true iff `;` occurs in the URL path before a `?`.
	*/
	hasParameterString() {
		return this.parameterStartIndex > 0;
	}
	/**
	* URL hash or fragment component.
	* See also: https://developer.mozilla.org/en-US/docs/Web/API/URL/hash
	*/
	get hash() {
		if (!this._search && !this._hash) this._extractParams();
		return this._hash;
	}
	get href() {
		return this._href;
	}
	/**
	* Returns the url (post parsing).
	* See also: https://developer.mozilla.org/en-US/docs/Web/API/URL/toString
	*/
	toString() {
		return this.href;
	}
	/**
	* JSONified URL (== toString)
	* See also: https://developer.mozilla.org/en-US/docs/Web/API/URL/toJSON
	*/
	toJSON() {
		return this.href;
	}
	/**
	* Get parsed domainInfo from the hostname.
	* @returns parsed domain, from tldts `parse` method.
	*/
	get domainInfo() {
		if (!this._domainInfo) this._domainInfo = parse(this.hostname, { extractHostname: false });
		return this._domainInfo;
	}
	/**
	* Returns true iff the hostname of this url is an IP address. False otherwise.
	*/
	get hostIsIp() {
		return this.domainInfo.isIp;
	}
	/**
	* Returns the hostname of the URL after parsing by tldts. This includes some error correction.
	*/
	get domain() {
		return this.domainInfo.hostname || this.hostname;
	}
	/**
	* Get eTLD+1 of the hostname.
	*/
	get generalDomain() {
		return this.domainInfo.domain || this.hostname;
	}
	/**
	* Legacy attribute for `pathname`.
	*/
	get path() {
		return this.pathname || "/";
	}
	/**
	* Scheme = protocol without a trailing ':'.
	*/
	get scheme() {
		return this.protocol.slice(0, -1);
	}
	/**
	* Check if the hostname of the URL is valid, i.e.
	*  * it is an IP address, or
	*  * it is a valid hostname with a known public suffix.
	* @returns true if host is valid, otherwise false.
	*/
	isValidHost() {
		return this.hostIsIp || this.generalDomain !== null;
	}
	/**
	* Non-standard params extractor.
	*
	* Returns search params from parameter string and query params with more aggessive extraction
	* than the standard URL implementation. Extra extraction features are:
	*  * `;` separated parameters - used by multi trackers
	* @returns URLSearchParams
	*/
	extractKeyValues() {
		if (this.parsedParameters) return this.parsedParameters;
		this.parsedParameters = new SearchParams();
		if (this.queryStartIndex === 0 && this.parameterStartIndex === 0) return this.parsedParameters;
		const start = this.parameterStartIndex || this.queryStartIndex;
		const end = this.href.length - 1;
		let index = start;
		if (this.href.charCodeAt(index) === 59) index = this._extractParamTuples(index + 1, end, this.parsedParameters, [59], 61, [63, 35]);
		if (this.href.charCodeAt(index) === 63) index = this._extractParamTuples(index + 1, end, this.parsedParameters, [38, 59], 61, [35]);
		return this.parsedParameters;
	}
	_extractHostname(start, end) {
		let portIndex = 0;
		let stopped = false;
		let i = start;
		let ipv6 = false;
		let hasUpper = false;
		if (this._href.charCodeAt(i) === 91) {
			ipv6 = true;
			for (; i <= end; i += 1) if (this._href.charCodeAt(i) === 93) {
				const nextCode = this._href.charCodeAt(i + 1);
				if (nextCode === 58) {
					portIndex = i + 1;
					i += 1;
					stopped = true;
				} else if (nextCode === 47) {
					i += 1;
					stopped = true;
				} else if (i !== end) throw new TypeError("expected `:` or `/` after IPv6 address");
				break;
			}
		}
		if (!ipv6) for (; i <= end; i += 1) {
			const code = this._href.charCodeAt(i);
			if (code === 58) {
				portIndex = i;
				stopped = true;
				break;
			} else if (code === 64) {
				this._username = this._href.slice(start, i);
				this._password = "";
				return this._extractHostname(i + 1, end);
			}
			if (BREAK_HOST_ON.indexOf(code) !== -1) {
				stopped = true;
				break;
			} else if (code <= 32) throw new TypeError(`Invalid character '${this.href[i]}' in hostname`);
			else if (code >= 65 && code <= 90) hasUpper = true;
		}
		const hostnameEnd = !stopped ? i + 1 : i;
		if (hasUpper) this._href = `${this._href.slice(0, start)}${this._href.slice(start, hostnameEnd).toLowerCase()}${this._href.slice(hostnameEnd)}`;
		this._hostname = this._href.slice(start, hostnameEnd);
		if (portIndex > 0) {
			i += 1;
			const portStart = i;
			let nonNumeric = false;
			for (; i <= end; i += 1) {
				const code = this._href.charCodeAt(i);
				if (BREAK_HOST_ON.indexOf(code) !== -1) {
					this._port = this._href.slice(portStart, i);
					break;
				} else if (code === 64) {
					this._username = this._href.slice(start, portIndex || i);
					this._password = this._href.slice(portIndex + 1, i);
					return this._extractHostname(i + 1, end);
				} else if (code < 48 || code > 57) nonNumeric = true;
			}
			if (!this._port) this._port = this.href.slice(portStart, i);
			if (nonNumeric) throw new TypeError("Invalid URL: port contains non numeric character");
			if (this._port.length >= 5 && +this._port > 65535) throw new TypeError("Invalid URL: invalid port number");
		}
		this._host = this._href.slice(start, !stopped ? i + 1 : i);
		this.origin = `${this._protocol}//${this._host}`;
		return !stopped ? i + 1 : i;
	}
	_extractParams() {
		if (this.queryStartIndex > 0) {
			let index = this.queryStartIndex;
			const end = this.href.length - 1;
			if (this.href.charCodeAt(index) === 63) {
				let broken = false;
				for (; index <= end; index += 1) if (this.href.charCodeAt(index) === 35) {
					broken = true;
					break;
				}
				this._search = this.href.slice(this.queryStartIndex, broken ? index : end + 1);
				if (this._search.length === 1) this._search = "";
			}
			if (this.href.charCodeAt(index) === 35) this._hash = this.href.slice(index, end + 1);
		}
	}
	_extractSearchParams() {
		this.isQueryParsed = true;
		if (this.queryStartIndex === 0 && this.parameterStartIndex === 0) return;
		const start = this.parameterStartIndex || this.queryStartIndex;
		const end = this.href.length - 1;
		let index = start;
		if (this.href.charCodeAt(index) === 59) index = this._extractParamTuples(index + 1, end, this._parameters, [59], 61, [63, 35]);
		if (this.href.charCodeAt(index) === 63) {
			const searchStart = index;
			index = this._extractParamTuples(index + 1, end, this._query, [38], 61, [35]);
			this._search = this.href.slice(searchStart, index);
			if (this._search.length === 1) this._search = "";
		}
		if (this.href.charCodeAt(index) === 35) this._hash = this.href.slice(index, end + 1);
	}
	_extractParamTuples(start, end, params, separators, equals, breakCodes) {
		return extractParams(this.href, start, end, params, separators, equals, breakCodes);
	}
	parse(url) {
		if (typeof url !== "string" || url.length === 0) throw new TypeError(`${url} is not a valid URL`);
		this._protocol = "";
		this._hostname = "";
		this._host = "";
		this._port = "";
		this._pathname = "";
		this._username = "";
		this._password = "";
		this._search = "";
		this._hash = "";
		this.parameterStartIndex = 0;
		this.queryStartIndex = 0;
		this.isQueryParsed = false;
		this._parameters = new SearchParams();
		this._query = new SearchParams();
		this._domainInfo = null;
		this.parsedParameters = null;
		let index = 0;
		let end = url.length - 1;
		while (url.charCodeAt(index) <= 32) index += 1;
		while (url.charCodeAt(end) <= 32) end -= 1;
		this._href = url.slice(index, end + 1);
		end = this._href.length - 1;
		let hasUpper = false;
		for (; index <= end; index += 1) {
			const code = this._href.charCodeAt(index);
			if (code === 58) {
				this._protocol = this._href.slice(0, index + 1);
				if (hasUpper) {
					this._protocol = this._protocol.toLowerCase();
					this._href = `${this._protocol}${this._href.slice(index + 1)}`;
				}
				break;
			} else if (!isValidProtocolChar(code)) throw new TypeError("Invalid URL protocol");
			else if (code >= 65 && code <= 90) hasUpper = true;
		}
		if (index >= end) throw new TypeError("No protocol");
		this.slashes = "";
		for (index += 1; index < end; index += 1) if (this._href.charCodeAt(index) !== 47) break;
		else this.slashes += "/";
		if (this.slashes.length >= 2) index = this._extractHostname(index, end);
		else {
			this._host = "";
			this._hostname = "";
			this.origin = "null";
		}
		if (index >= end) {
			if (this._href.charCodeAt(end) !== 47) this._href += "/";
			this._pathname = "/";
		} else {
			const pathStart = index;
			for (; index <= end; index += 1) {
				const code = this._href.charCodeAt(index);
				if (code === 59 && !this.parameterStartIndex) this.parameterStartIndex = index;
				else if (code === 63 || code === 35) {
					this.queryStartIndex = index;
					break;
				}
			}
			this._pathname = this.href.slice(pathStart, this.queryStartIndex !== 0 ? this.queryStartIndex : end + 1) || "/";
		}
	}
};
//#endregion
export { immutable_url_default as default };
