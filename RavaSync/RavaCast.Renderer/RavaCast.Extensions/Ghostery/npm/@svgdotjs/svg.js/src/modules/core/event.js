import { globals } from "../../utils/window.js";
import { makeInstance } from "../../utils/adopter.js";
import { delimiter } from "./regex.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/event.js
var listenerId = 0;
var windowEvents = {};
function getEvents(instance) {
	let n = instance.getEventHolder();
	if (n === globals.window) n = windowEvents;
	if (!n.events) n.events = {};
	return n.events;
}
function getEventTarget(instance) {
	return instance.getEventTarget();
}
function clearEvents(instance) {
	let n = instance.getEventHolder();
	if (n === globals.window) n = windowEvents;
	if (n.events) n.events = {};
}
function on(node, events, listener, binding, options) {
	const l = listener.bind(binding || node);
	const instance = makeInstance(node);
	const bag = getEvents(instance);
	const n = getEventTarget(instance);
	events = Array.isArray(events) ? events : events.split(delimiter);
	if (!listener._svgjsListenerId) listener._svgjsListenerId = ++listenerId;
	events.forEach(function(event) {
		const ev = event.split(".")[0];
		const ns = event.split(".")[1] || "*";
		bag[ev] = bag[ev] || {};
		bag[ev][ns] = bag[ev][ns] || {};
		bag[ev][ns][listener._svgjsListenerId] = l;
		n.addEventListener(ev, l, options || false);
	});
}
function off(node, events, listener, options) {
	const instance = makeInstance(node);
	const bag = getEvents(instance);
	const n = getEventTarget(instance);
	if (typeof listener === "function") {
		listener = listener._svgjsListenerId;
		if (!listener) return;
	}
	events = Array.isArray(events) ? events : (events || "").split(delimiter);
	events.forEach(function(event) {
		const ev = event && event.split(".")[0];
		const ns = event && event.split(".")[1];
		let namespace, l;
		if (listener) {
			if (bag[ev] && bag[ev][ns || "*"]) {
				n.removeEventListener(ev, bag[ev][ns || "*"][listener], options || false);
				delete bag[ev][ns || "*"][listener];
			}
		} else if (ev && ns) {
			if (bag[ev] && bag[ev][ns]) {
				for (l in bag[ev][ns]) off(n, [ev, ns].join("."), l);
				delete bag[ev][ns];
			}
		} else if (ns) {
			for (event in bag) for (namespace in bag[event]) if (ns === namespace) off(n, [event, ns].join("."));
		} else if (ev) {
			if (bag[ev]) {
				for (namespace in bag[ev]) off(n, [ev, namespace].join("."));
				delete bag[ev];
			}
		} else {
			for (event in bag) off(n, event);
			clearEvents(instance);
		}
	});
}
function dispatch(node, event, data, options) {
	const n = getEventTarget(node);
	if (event instanceof globals.window.Event) n.dispatchEvent(event);
	else {
		event = new globals.window.CustomEvent(event, {
			detail: data,
			cancelable: true,
			...options
		});
		n.dispatchEvent(event);
	}
	return event;
}
//#endregion
export { clearEvents, dispatch, getEventTarget, getEvents, off, on, windowEvents };
