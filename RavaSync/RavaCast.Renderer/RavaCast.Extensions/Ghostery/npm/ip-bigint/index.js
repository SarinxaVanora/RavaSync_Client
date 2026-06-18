//#region node_modules/ip-bigint/index.js
function parseIp(ip) {
	const version = ipVersion(ip);
	if (!version) throw new Error(`Invalid IP address: ${ip}`);
	let number = 0n;
	let exp = 0n;
	const res = Object.create(null);
	if (version === 4) for (const n of ip.split(".").map(BigInt).reverse()) {
		number += n * 2n ** exp;
		exp += 8n;
	}
	else {
		if (ip.includes(".")) {
			res.ipv4mapped = true;
			ip = ip.split(":").map((part) => {
				if (part.includes(".")) {
					const digits = part.split(".").map((str) => Number(str).toString(16).padStart(2, "0"));
					return `${digits[0]}${digits[1]}:${digits[2]}${digits[3]}`;
				} else return part;
			}).join(":");
		}
		if (ip.includes("%")) {
			let scopeid;
			[, ip, scopeid] = /(.+)%(.+)/.exec(ip);
			res.scopeid = scopeid;
		}
		const parts = ip.split(":");
		const index = parts.indexOf("");
		if (index !== -1) while (parts.length < 8) parts.splice(index, 0, "");
		for (const n of parts.map((part) => BigInt(parseInt(part || 0, 16))).reverse()) {
			number += n * 2n ** exp;
			exp += 16n;
		}
	}
	res.number = number;
	res.version = version;
	return res;
}
function ipVersion(ip) {
	return ip.includes(":") ? 6 : ip.includes(".") ? 4 : 0;
}
//#endregion
export { parseIp };
