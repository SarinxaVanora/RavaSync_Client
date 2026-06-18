//#region node_modules/@whotracksme/reporting/reporting/src/request/utils/utils.js
/**
* WhoTracks.Me
* https://whotracks.me/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
function truncateDomain(host, depth) {
	const generalDomain = host.domain;
	if (host.isIp || host.hostname === generalDomain || generalDomain === null || generalDomain.length === 0) return host.hostname;
	const subdomains = host.subdomain.split(".").filter((p) => p.length > 0);
	return `${subdomains.slice(Math.max(subdomains.length - depth, 0)).join(".")}.${generalDomain}`;
}
var ipv4Part = "0*([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])";
var ipv4Regex = new RegExp(`^${ipv4Part}\\.${ipv4Part}\\.${ipv4Part}\\.${ipv4Part}([:]([0-9])+)?$`);
function isIpv4Address(host) {
	return ipv4Regex.test(host);
}
//#endregion
export { isIpv4Address, truncateDomain };
