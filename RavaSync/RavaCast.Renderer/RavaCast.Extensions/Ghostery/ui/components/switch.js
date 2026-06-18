import children from "../../npm/hybrids/src/children.js";
import { html } from "../../npm/hybrids/src/template/index.js";
import switch_item_default from "./switch-item.js";
//#region src/ui/components/switch.js
var switch_default = {
	height: {
		value: 0,
		observe(host, value, lastValue) {
			if (lastValue) {
				host.style.height = `${lastValue}px`;
				requestAnimationFrame(() => {
					host.style.height = `${value}px`;
					host.addEventListener("transitionend", (event) => {
						if (event.currentTarget === host) host.style.height = "";
					}, { once: true });
				});
			}
		}
	},
	items: children(switch_item_default),
	active: {
		value: ({ items }) => items.find((item) => item.active),
		observe(host) {
			requestAnimationFrame(() => {
				host.height = host.clientHeight;
			});
		}
	},
	render: () => html`
    <template layout="block relative overflow:x margin:0:-2 padding:0:2">
      <slot></slot>
    </template>
  `.css`
    :host {
      box-sizing: border-box;
      transition: height 500ms cubic-bezier(0.4, 0.15, 0, 1);
      will-change: height;
      overflow: hidden;
    }
  `
};
//#endregion
export { switch_default as default };
