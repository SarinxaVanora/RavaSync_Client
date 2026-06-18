import { globals } from "../utils/window.js";
import Queue from "./Queue.js";
//#region node_modules/@svgdotjs/svg.js/src/animation/Animator.js
var Animator = {
	nextDraw: null,
	frames: new Queue(),
	timeouts: new Queue(),
	immediates: new Queue(),
	timer: () => globals.window.performance || globals.window.Date,
	transforms: [],
	frame(fn) {
		const node = Animator.frames.push({ run: fn });
		if (Animator.nextDraw === null) Animator.nextDraw = globals.window.requestAnimationFrame(Animator._draw);
		return node;
	},
	timeout(fn, delay) {
		delay = delay || 0;
		const time = Animator.timer().now() + delay;
		const node = Animator.timeouts.push({
			run: fn,
			time
		});
		if (Animator.nextDraw === null) Animator.nextDraw = globals.window.requestAnimationFrame(Animator._draw);
		return node;
	},
	immediate(fn) {
		const node = Animator.immediates.push(fn);
		if (Animator.nextDraw === null) Animator.nextDraw = globals.window.requestAnimationFrame(Animator._draw);
		return node;
	},
	cancelFrame(node) {
		node != null && Animator.frames.remove(node);
	},
	clearTimeout(node) {
		node != null && Animator.timeouts.remove(node);
	},
	cancelImmediate(node) {
		node != null && Animator.immediates.remove(node);
	},
	_draw(now) {
		let nextTimeout = null;
		const lastTimeout = Animator.timeouts.last();
		while (nextTimeout = Animator.timeouts.shift()) {
			if (now >= nextTimeout.time) nextTimeout.run();
			else Animator.timeouts.push(nextTimeout);
			if (nextTimeout === lastTimeout) break;
		}
		let nextFrame = null;
		const lastFrame = Animator.frames.last();
		while (nextFrame !== lastFrame && (nextFrame = Animator.frames.shift())) nextFrame.run(now);
		let nextImmediate = null;
		while (nextImmediate = Animator.immediates.shift()) nextImmediate();
		Animator.nextDraw = Animator.timeouts.first() || Animator.frames.first() ? globals.window.requestAnimationFrame(Animator._draw) : null;
	}
};
//#endregion
export { Animator as default };
