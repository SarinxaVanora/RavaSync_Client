import store_default from "../../../npm/hybrids/src/store.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import { getCategoryColor } from "../../../ui/categories.js";
import { categories } from "../../../ui/labels.js";
import { ApexSankey } from "../../../npm/apexsankey/apexsankey.es.min.js";
import { MergedStats } from "../../../store/daily-stats.js";
//#region src/pages/whotracksme/components/sankey-chart.js
/**
* Ghostery Browser Extension
* https://www.ghostery.com/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
var sankey_chart_default = {
	dateFrom: "",
	dateTo: "",
	slice: 10,
	stats: store_default(MergedStats, { id: ({ dateFrom, dateTo }) => ({
		dateFrom,
		dateTo
	}) }),
	chart: ({ render }) => {
		return new ApexSankey(render().querySelector("#chart"), {
			height: "100",
			canvasStyle: "box-sizing: border-box;",
			nodeWidth: 0,
			spacing: 0,
			fontSize: "10px",
			fontFamily: "Inter",
			fontWeight: "600",
			fontColor: "var(--color-primary)",
			edgeGradientFill: false,
			edgeOpacity: .4,
			enableToolbar: false,
			viewPortHeight: 280,
			viewPortWidth: 800,
			tooltipBorderColor: "var(--border-primary)",
			tooltipBGColor: "var(--background-primary)"
		});
	},
	data: {
		value: ({ stats, slice }) => {
			if (!store_default.ready(stats) || store_default.pending(stats)) return void 0;
			return stats.groupedTrackers.slice(0, slice).reduce((data, tracker) => {
				if (!tracker.organization) return data;
				if (!data.nodes.find((node) => node.id === tracker.category)) data.nodes.push({
					id: tracker.category,
					title: categories[tracker.category],
					color: getCategoryColor(tracker.category)
				});
				if (!data.nodes.find((node) => node.id === tracker.organization.id)) data.nodes.push({
					id: tracker.organization.id,
					title: () => store_default.resolve(tracker.organization).then((org) => org.name),
					color: "var(--background-tertiary)"
				});
				let edge = data.edges.find((edge) => edge.source === tracker.category && edge.target === tracker.organization.id);
				if (edge) {
					edge.value += stats.trackers.reduce((acc, id) => acc + (id === tracker.id ? 1 : 0), 0);
					return data;
				}
				data.edges.push({
					source: tracker.category,
					target: tracker.organization.id,
					value: stats.trackers.reduce((acc, id) => acc + (id === tracker.id ? 1 : 0), 0)
				});
				return data;
			}, {
				nodes: [],
				edges: []
			});
		},
		connect(host, _, invalidate) {
			const matchMedia = window.matchMedia("(prefers-color-scheme: dark)");
			matchMedia.addEventListener("change", invalidate);
			return () => matchMedia.removeEventListener("change", invalidate);
		},
		async observe(host, data) {
			if (data === void 0) return;
			host.chart.options.viewPortHeight = Math.min(Math.max(data.edges.reduce((acc, edge) => acc + edge.value, 0), 280), 360);
			for (const node of data.nodes) if (typeof node.title === "function") node.title = await node.title();
			host.chart.render(data);
		}
	},
	render: () => html`
    <template layout="column gap:2">
      <div layout="row content:space-between padding:0:1">
        <ui-text type="label-m">Categories</ui-text>
        <ui-text type="label-m">Organizations</ui-text>
      </div>
      <div id="chart"></div>
    </template>
  `
};
//#endregion
export { sankey_chart_default as default };
