import { __toESM } from "../../../../../../virtual/_rolldown/runtime.js";
import { AdblockSyntax } from "../../utils/adblockers.js";
import { StringUtils } from "../../utils/string.js";
import { NetworkRuleType, RuleCategory } from "../../nodes/index.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "../misc/value-parser.js";
import { getDomain, getHostname } from "../../../../../tldts/dist/es6/index.js";
import { require_is_ip } from "../../../../../is-ip/index.js";
//#region node_modules/@adguard/agtree/dist/parser/network/host-rule-parser.js
var import_is_ip = /* @__PURE__ */ __toESM(require_is_ip(), 1);
/**
* `HostRuleParser` is responsible for parsing hosts-like rules.
*
* HostRule is a structure for simple host-level rules (i.e. /etc/hosts syntax).
* It also supports "just domain" syntax. In this case, the IP will be set to 0.0.0.0.
*
* Rules syntax looks like this:
* ```text
* IP_address canonical_hostname [aliases...]
* ```
*
* @example
* `192.168.1.13 bar.mydomain.org bar` -- ipv4
* `ff02::1 ip6-allnodes` -- ipv6
* `::1 localhost ip6-localhost ip6-loopback` -- ipv6 aliases
* `example.org` -- "just domain" syntax
* @see {@link http://man7.org/linux/man-pages/man5/hosts.5.html}
*/
var HostRuleParser = class HostRuleParser extends BaseParser {
	static NULL_IP = "0.0.0.0";
	static COMMENT_MARKER = "#";
	/**
	* Parses an etc/hosts-like rule.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Host rule node.
	*
	* @throws If the input contains invalid data.
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let offset = StringUtils.skipWS(raw, 0);
		const parts = [];
		let lastPartStartIndex = offset;
		let comment = null;
		const rawLength = raw.length;
		const parsePartIfNeeded = (startIndex, endIndex) => {
			if (startIndex < endIndex) parts.push(ValueParser.parse(raw.slice(startIndex, endIndex), options, baseOffset + startIndex));
		};
		while (offset < rawLength) if (StringUtils.isWhitespace(raw[offset])) {
			parsePartIfNeeded(lastPartStartIndex, offset);
			offset = StringUtils.skipWS(raw, offset);
			lastPartStartIndex = offset;
		} else if (raw[offset] === HostRuleParser.COMMENT_MARKER) {
			const commentStart = offset;
			offset = StringUtils.skipWS(raw, offset + 1);
			comment = ValueParser.parse(raw.slice(offset), options, baseOffset + commentStart);
			offset = rawLength;
			lastPartStartIndex = offset;
		} else offset += 1;
		parsePartIfNeeded(lastPartStartIndex, offset);
		const partsLength = parts.length;
		if (partsLength < 1) throw new Error("Host rule must have at least one domain name or an IP address and a domain name");
		const result = {
			category: RuleCategory.Network,
			type: NetworkRuleType.HostRule,
			syntax: AdblockSyntax.Common
		};
		if (partsLength === 1) {
			if (getDomain(parts[0].value) !== parts[0].value) throw new Error(`Not a valid domain: ${parts[0].value}`);
			result.ip = {
				type: "Value",
				value: HostRuleParser.NULL_IP
			};
			result.hostnames = {
				type: "HostnameList",
				children: parts
			};
		} else if (partsLength > 1) {
			const [ip, ...hostnames] = parts;
			if (!(0, import_is_ip.default)(ip.value)) throw new Error(`Invalid IP address: ${ip.value}`);
			for (const { value } of hostnames) if (getHostname(value) !== value) throw new Error(`Not a valid hostname: ${value}`);
			result.ip = ip;
			result.hostnames = {
				type: "HostnameList",
				children: hostnames
			};
		}
		if (comment) result.comment = comment;
		if (options.includeRaws) result.raws = { text: raw };
		return result;
	}
};
//#endregion
export { HostRuleParser };
