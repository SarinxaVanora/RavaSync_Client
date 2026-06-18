import "../../virtual/_rolldown/runtime.js";
import ipRegex from "../ip-regex/index.js";
import cidrRegex from "../cidr-regex/index.js";
import { require_natural_compare } from "../string-natural-compare/natural-compare.js";
import { parseIp } from "../ip-bigint/index.js";
require_natural_compare();
var bits = {
	4: 32,
	6: 128
};
var uniq = (arr) => Array.from(new Set(arr));
function isIP(ip) {
	if (ipRegex.v4({ exact: true }).test(ip)) return 4;
	if (ipRegex.v6({ exact: true }).test(ip)) return 6;
	return 0;
}
function isCidr(ip) {
	if (cidrRegex.v4({ exact: true }).test(ip)) return 4;
	if (cidrRegex.v6({ exact: true }).test(ip)) return 6;
	return 0;
}
function parse(str) {
	const cidrVersion = isCidr(str);
	const parsed = Object.create(null);
	parsed.single = false;
	if (cidrVersion) {
		parsed.cidr = str;
		parsed.version = cidrVersion;
	} else {
		const version = isIP(str);
		if (version) {
			parsed.cidr = `${str}/${bits[version]}`;
			parsed.version = version;
			parsed.single = true;
		} else throw new Error(`Network is not a CIDR or IP: ${str}`);
	}
	const [ip, prefix] = parsed.cidr.split("/");
	parsed.prefix = prefix;
	const { number, version } = parseIp(ip);
	const numBits = bits[version];
	const ipBits = number.toString(2).padStart(numBits, "0");
	const prefixLen = Number(numBits - prefix);
	const startBits = ipBits.substring(0, numBits - prefixLen);
	parsed.start = BigInt(`0b${startBits}${"0".repeat(prefixLen)}`);
	parsed.end = BigInt(`0b${startBits}${"1".repeat(prefixLen)}`);
	return parsed;
}
function netContains(a, b) {
	if (b.start < a.start) return false;
	if (b.end > a.end) return false;
	return true;
}
function contains(a, b) {
	const aNets = uniq(Array.isArray(a) ? a : [a]);
	const bNets = uniq(Array.isArray(b) ? b : [b]);
	const numExpected = bNets.length;
	let numFound = 0;
	for (const a of aNets) {
		const aParsed = parse(a);
		for (const b of bNets) {
			const bParsed = parse(b);
			if (aParsed.version !== bParsed.version) continue;
			if (netContains(aParsed, bParsed)) {
				numFound++;
				continue;
			}
		}
	}
	return numFound === numExpected;
}
//#endregion
export { contains };
