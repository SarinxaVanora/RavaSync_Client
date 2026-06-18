//#region src/utils/tabs.js
async function openHref(host, event) {
	const { href } = event.currentTarget;
	event.preventDefault();
	await openTabWithUrl(href);
	window.close();
}
async function openTabWithUrl(url) {
	try {
		const tabs = await chrome.tabs.query({
			url: url.split("#")[0],
			currentWindow: true
		});
		if (tabs.length) {
			await chrome.tabs.update(tabs[0].id, {
				active: true,
				url: url !== tabs[0].url ? url : void 0
			});
			return;
		}
	} catch (e) {
		console.error("[utils|tabs] Error while try to find existing tab:", e);
	}
	await chrome.tabs.create({ url });
}
async function getCurrentTab() {
	const [tab] = await chrome.tabs.query({
		active: true,
		currentWindow: true
	});
	return tab || null;
}
//#endregion
export { getCurrentTab, openHref, openTabWithUrl };
