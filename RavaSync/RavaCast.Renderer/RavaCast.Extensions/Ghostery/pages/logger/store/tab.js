import store_default from "../../../npm/hybrids/src/store.js";
//#region src/pages/logger/store/tab.js
var tab_default = {
	id: true,
	title: "",
	hostname: "",
	active: false,
	[store_default.connect]: { async list() {
		return (await chrome.tabs.query({})).filter(({ url }) => url !== location.href).map((tab) => ({
			id: tab.id,
			title: tab.title,
			hostname: new URL(tab.url).hostname,
			active: tab.active
		}));
	} }
};
//#endregion
export { tab_default as default };
