import { __toESM } from "../../../virtual/_rolldown/runtime.js";
import store_default from "../../../npm/hybrids/src/store.js";
import { html } from "../../../npm/hybrids/src/template/index.js";
import DailyStats from "../../../store/daily-stats.js";
import { require_plotly_basic } from "../../../npm/plotly.js-basic-dist/plotly-basic.js";
//#region src/pages/whotracksme/components/trends-chart.js
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
var import_plotly_basic = /* @__PURE__ */ __toESM(require_plotly_basic(), 1);
var TRACE_COLORS = {
	"pages": "--component-chart-line-observed",
	"trackersBlocked": "--component-chart-line-blocked",
	"trackersModified": "--component-chart-line-modified"
};
var trends_chart_default = {
	dateFrom: "",
	dateTo: "",
	stats: store_default([DailyStats], { id: ({ dateFrom, dateTo }) => ({
		dateFrom,
		dateTo
	}) }),
	trends: { value: [] },
	aggregate: 0,
	data: {
		value: ({ stats, trends, aggregate }) => {
			if (!store_default.ready(stats) || store_default.pending(stats)) return void 0;
			return stats.length ? trends.map((key) => {
				let index = -1;
				let lastDay = null;
				const { x, y } = stats.reduce((acc, { day, [key]: value }) => {
					if (acc.x.length === 0 || !aggregate || new Date(day) - lastDay >= aggregate * 24 * 60 * 60 * 1e3) {
						acc.x.push(day);
						acc.y.push(value);
						index += 1;
						lastDay = new Date(day);
						return acc;
					}
					acc.y[index] = (acc.y[index] || 0) + value;
					return acc;
				}, {
					x: [],
					y: []
				});
				return {
					name: key,
					x,
					y,
					type: "scatter",
					mode: "lines",
					text: x.map((day, index) => `${day} - ${y[index]}`),
					hoverinfo: "text"
				};
			}) : [];
		},
		connect(host, _, invalidate) {
			const matchMedia = window.matchMedia("(prefers-color-scheme: dark)");
			matchMedia.addEventListener("change", invalidate);
			return () => matchMedia.removeEventListener("change", invalidate);
		},
		observe(host, data) {
			if (data === void 0) return;
			const chart = host.render().querySelector("#chart");
			import_plotly_basic.default.purge(chart);
			window.requestAnimationFrame(() => {
				const computedStyle = window.getComputedStyle(host);
				import_plotly_basic.default.newPlot(chart, data.map((trace) => {
					trace.line = {
						color: computedStyle.getPropertyValue(TRACE_COLORS[trace.name]),
						width: 2
					};
					return trace;
				}), {
					dragmode: false,
					autosize: true,
					margin: {
						b: 0,
						l: 0,
						r: 0,
						t: 0,
						pad: 20
					},
					showlegend: false,
					yaxis: {
						fixedrange: true,
						automargin: true,
						color: computedStyle.getPropertyValue("--color-tertiary"),
						gridcolor: computedStyle.getPropertyValue("--border-primary"),
						zerolinecolor: computedStyle.getPropertyValue("--border-primary"),
						tickfont: {
							size: 10,
							family: "Inter"
						}
					},
					xaxis: {
						fixedrange: true,
						automargin: true,
						showgrid: false,
						showline: false,
						zeroline: false,
						ticklabelposition: "outside",
						tickfont: {
							size: 10,
							family: "Inter"
						},
						color: computedStyle.getPropertyValue("--color-tertiary")
					},
					bargap: .4,
					hoverlabel: { font: {
						family: "Inter",
						size: 12
					} },
					template: { layout: {
						paper_bgcolor: "transparent",
						plot_bgcolor: "transparent"
					} }
				}, {
					displayModeBar: false,
					responsive: true
				});
			});
		}
	},
	render: () => html`
    <template layout="grid">
      <div id="chart"></div>
    </template>
  `
};
//#endregion
export { trends_chart_default as default };
