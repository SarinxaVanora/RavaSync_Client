import { registerMethods } from "../utils/methods.js";
import { getOrigin } from "../utils/utils.js";
import { extend, register } from "../utils/adopter.js";
import Point from "../types/Point.js";
import Matrix from "../types/Matrix.js";
import Box from "../types/Box.js";
import EventTarget from "../types/EventTarget.js";
import { noop, timeline } from "../modules/core/defaults.js";
import SVGNumber from "../types/SVGNumber.js";
import { rx, ry } from "../modules/core/circled.js";
import { from, to } from "../modules/core/gradiented.js";
import { Controller, Ease, Stepper } from "./Controller.js";
import Morphable, { ObjectBag, TransformBag } from "./Morphable.js";
import Animator from "./Animator.js";
import Timeline from "./Timeline.js";
//#region node_modules/@svgdotjs/svg.js/src/animation/Runner.js
var Runner = class Runner extends EventTarget {
	constructor(options) {
		super();
		this.id = Runner.id++;
		options = options == null ? timeline.duration : options;
		options = typeof options === "function" ? new Controller(options) : options;
		this._element = null;
		this._timeline = null;
		this.done = false;
		this._queue = [];
		this._duration = typeof options === "number" && options;
		this._isDeclarative = options instanceof Controller;
		this._stepper = this._isDeclarative ? options : new Ease();
		this._history = {};
		this.enabled = true;
		this._time = 0;
		this._lastTime = 0;
		this._reseted = true;
		this.transforms = new Matrix();
		this.transformId = 1;
		this._haveReversed = false;
		this._reverse = false;
		this._loopsDone = 0;
		this._swing = false;
		this._wait = 0;
		this._times = 1;
		this._frameId = null;
		this._persist = this._isDeclarative ? true : null;
	}
	static sanitise(duration, delay, when) {
		let times = 1;
		let swing = false;
		let wait = 0;
		duration = duration ?? timeline.duration;
		delay = delay ?? timeline.delay;
		when = when || "last";
		if (typeof duration === "object" && !(duration instanceof Stepper)) {
			delay = duration.delay ?? delay;
			when = duration.when ?? when;
			swing = duration.swing || swing;
			times = duration.times ?? times;
			wait = duration.wait ?? wait;
			duration = duration.duration ?? timeline.duration;
		}
		return {
			duration,
			delay,
			swing,
			times,
			wait,
			when
		};
	}
	active(enabled) {
		if (enabled == null) return this.enabled;
		this.enabled = enabled;
		return this;
	}
	addTransform(transform) {
		this.transforms.lmultiplyO(transform);
		return this;
	}
	after(fn) {
		return this.on("finished", fn);
	}
	animate(duration, delay, when) {
		const o = Runner.sanitise(duration, delay, when);
		const runner = new Runner(o.duration);
		if (this._timeline) runner.timeline(this._timeline);
		if (this._element) runner.element(this._element);
		return runner.loop(o).schedule(o.delay, o.when);
	}
	clearTransform() {
		this.transforms = new Matrix();
		return this;
	}
	clearTransformsFromQueue() {
		if (!this.done || !this._timeline || !this._timeline._runnerIds.includes(this.id)) this._queue = this._queue.filter((item) => {
			return !item.isTransform;
		});
	}
	delay(delay) {
		return this.animate(0, delay);
	}
	duration() {
		return this._times * (this._wait + this._duration) - this._wait;
	}
	during(fn) {
		return this.queue(null, fn);
	}
	ease(fn) {
		this._stepper = new Ease(fn);
		return this;
	}
	element(element) {
		if (element == null) return this._element;
		this._element = element;
		element._prepareRunner();
		return this;
	}
	finish() {
		return this.step(Infinity);
	}
	loop(times, swing, wait) {
		if (typeof times === "object") {
			swing = times.swing;
			wait = times.wait;
			times = times.times;
		}
		this._times = times || Infinity;
		this._swing = swing || false;
		this._wait = wait || 0;
		if (this._times === true) this._times = Infinity;
		return this;
	}
	loops(p) {
		const loopDuration = this._duration + this._wait;
		if (p == null) {
			const loopsDone = Math.floor(this._time / loopDuration);
			const position = (this._time - loopsDone * loopDuration) / this._duration;
			return Math.min(loopsDone + position, this._times);
		}
		const whole = Math.floor(p);
		const partial = p % 1;
		const time = loopDuration * whole + this._duration * partial;
		return this.time(time);
	}
	persist(dtOrForever) {
		if (dtOrForever == null) return this._persist;
		this._persist = dtOrForever;
		return this;
	}
	position(p) {
		const x = this._time;
		const d = this._duration;
		const w = this._wait;
		const t = this._times;
		const s = this._swing;
		const r = this._reverse;
		let position;
		if (p == null) {
			const f = function(x) {
				const swinging = s * Math.floor(x % (2 * (w + d)) / (w + d));
				const backwards = swinging && !r || !swinging && r;
				const uncliped = Math.pow(-1, backwards) * (x % (w + d)) / d + backwards;
				return Math.max(Math.min(uncliped, 1), 0);
			};
			const endTime = t * (w + d) - w;
			position = x <= 0 ? Math.round(f(1e-5)) : x < endTime ? f(x) : Math.round(f(endTime - 1e-5));
			return position;
		}
		const loopsDone = Math.floor(this.loops());
		const swingForward = s && loopsDone % 2 === 0;
		position = loopsDone + (swingForward && !r || r && swingForward ? p : 1 - p);
		return this.loops(position);
	}
	progress(p) {
		if (p == null) return Math.min(1, this._time / this.duration());
		return this.time(p * this.duration());
	}
	queue(initFn, runFn, retargetFn, isTransform) {
		this._queue.push({
			initialiser: initFn || noop,
			runner: runFn || noop,
			retarget: retargetFn,
			isTransform,
			initialised: false,
			finished: false
		});
		this.timeline() && this.timeline()._continue();
		return this;
	}
	reset() {
		if (this._reseted) return this;
		this.time(0);
		this._reseted = true;
		return this;
	}
	reverse(reverse) {
		this._reverse = reverse == null ? !this._reverse : reverse;
		return this;
	}
	schedule(timeline, delay, when) {
		if (!(timeline instanceof Timeline)) {
			when = delay;
			delay = timeline;
			timeline = this.timeline();
		}
		if (!timeline) throw Error("Runner cannot be scheduled without timeline");
		timeline.schedule(this, delay, when);
		return this;
	}
	step(dt) {
		if (!this.enabled) return this;
		dt = dt == null ? 16 : dt;
		this._time += dt;
		const position = this.position();
		const running = this._lastPosition !== position && this._time >= 0;
		this._lastPosition = position;
		const duration = this.duration();
		const justStarted = this._lastTime <= 0 && this._time > 0;
		const justFinished = this._lastTime < duration && this._time >= duration;
		this._lastTime = this._time;
		if (justStarted) this.fire("start", this);
		const declarative = this._isDeclarative;
		this.done = !declarative && !justFinished && this._time >= duration;
		this._reseted = false;
		let converged = false;
		if (running || declarative) {
			this._initialise(running);
			this.transforms = new Matrix();
			converged = this._run(declarative ? dt : position);
			this.fire("step", this);
		}
		this.done = this.done || converged && declarative;
		if (justFinished) this.fire("finished", this);
		return this;
	}
	time(time) {
		if (time == null) return this._time;
		const dt = time - this._time;
		this.step(dt);
		return this;
	}
	timeline(timeline) {
		if (typeof timeline === "undefined") return this._timeline;
		this._timeline = timeline;
		return this;
	}
	unschedule() {
		const timeline = this.timeline();
		timeline && timeline.unschedule(this);
		return this;
	}
	_initialise(running) {
		if (!running && !this._isDeclarative) return;
		for (let i = 0, len = this._queue.length; i < len; ++i) {
			const current = this._queue[i];
			const needsIt = this._isDeclarative || !current.initialised && running;
			running = !current.finished;
			if (needsIt && running) {
				current.initialiser.call(this);
				current.initialised = true;
			}
		}
	}
	_rememberMorpher(method, morpher) {
		this._history[method] = {
			morpher,
			caller: this._queue[this._queue.length - 1]
		};
		if (this._isDeclarative) {
			const timeline = this.timeline();
			timeline && timeline.play();
		}
	}
	_run(positionOrDt) {
		let allfinished = true;
		for (let i = 0, len = this._queue.length; i < len; ++i) {
			const current = this._queue[i];
			const converged = current.runner.call(this, positionOrDt);
			current.finished = current.finished || converged === true;
			allfinished = allfinished && current.finished;
		}
		return allfinished;
	}
	_tryRetarget(method, target, extra) {
		if (this._history[method]) {
			if (!this._history[method].caller.initialised) {
				const index = this._queue.indexOf(this._history[method].caller);
				this._queue.splice(index, 1);
				return false;
			}
			if (this._history[method].caller.retarget) this._history[method].caller.retarget.call(this, target, extra);
			else this._history[method].morpher.to(target);
			this._history[method].caller.finished = false;
			const timeline = this.timeline();
			timeline && timeline.play();
			return true;
		}
		return false;
	}
};
Runner.id = 0;
var FakeRunner = class {
	constructor(transforms = new Matrix(), id = -1, done = true) {
		this.transforms = transforms;
		this.id = id;
		this.done = done;
	}
	clearTransformsFromQueue() {}
};
extend([Runner, FakeRunner], { mergeWith(runner) {
	return new FakeRunner(runner.transforms.lmultiply(this.transforms), runner.id);
} });
var lmultiply = (last, curr) => last.lmultiplyO(curr);
var getRunnerTransform = (runner) => runner.transforms;
function mergeTransforms() {
	const netTransform = this._transformationRunners.runners.map(getRunnerTransform).reduce(lmultiply, new Matrix());
	this.transform(netTransform);
	this._transformationRunners.merge();
	if (this._transformationRunners.length() === 1) this._frameId = null;
}
var RunnerArray = class {
	constructor() {
		this.runners = [];
		this.ids = [];
	}
	add(runner) {
		if (this.runners.includes(runner)) return;
		const id = runner.id + 1;
		this.runners.push(runner);
		this.ids.push(id);
		return this;
	}
	clearBefore(id) {
		const deleteCnt = this.ids.indexOf(id + 1) || 1;
		this.ids.splice(0, deleteCnt, 0);
		this.runners.splice(0, deleteCnt, new FakeRunner()).forEach((r) => r.clearTransformsFromQueue());
		return this;
	}
	edit(id, newRunner) {
		const index = this.ids.indexOf(id + 1);
		this.ids.splice(index, 1, id + 1);
		this.runners.splice(index, 1, newRunner);
		return this;
	}
	getByID(id) {
		return this.runners[this.ids.indexOf(id + 1)];
	}
	length() {
		return this.ids.length;
	}
	merge() {
		let lastRunner = null;
		for (let i = 0; i < this.runners.length; ++i) {
			const runner = this.runners[i];
			if (lastRunner && runner.done && lastRunner.done && (!runner._timeline || !runner._timeline._runnerIds.includes(runner.id)) && (!lastRunner._timeline || !lastRunner._timeline._runnerIds.includes(lastRunner.id))) {
				this.remove(runner.id);
				const newRunner = runner.mergeWith(lastRunner);
				this.edit(lastRunner.id, newRunner);
				lastRunner = newRunner;
				--i;
			} else lastRunner = runner;
		}
		return this;
	}
	remove(id) {
		const index = this.ids.indexOf(id + 1);
		this.ids.splice(index, 1);
		this.runners.splice(index, 1);
		return this;
	}
};
registerMethods({ Element: {
	animate(duration, delay, when) {
		const o = Runner.sanitise(duration, delay, when);
		const timeline = this.timeline();
		return new Runner(o.duration).loop(o).element(this).timeline(timeline.play()).schedule(o.delay, o.when);
	},
	delay(by, when) {
		return this.animate(0, by, when);
	},
	_clearTransformRunnersBefore(currentRunner) {
		this._transformationRunners.clearBefore(currentRunner.id);
	},
	_currentTransform(current) {
		return this._transformationRunners.runners.filter((runner) => runner.id <= current.id).map(getRunnerTransform).reduce(lmultiply, new Matrix());
	},
	_addRunner(runner) {
		this._transformationRunners.add(runner);
		Animator.cancelImmediate(this._frameId);
		this._frameId = Animator.immediate(mergeTransforms.bind(this));
	},
	_prepareRunner() {
		if (this._frameId == null) this._transformationRunners = new RunnerArray().add(new FakeRunner(new Matrix(this)));
	}
} });
var difference = (a, b) => a.filter((x) => !b.includes(x));
extend(Runner, {
	attr(a, v) {
		return this.styleAttr("attr", a, v);
	},
	css(s, v) {
		return this.styleAttr("css", s, v);
	},
	styleAttr(type, nameOrAttrs, val) {
		if (typeof nameOrAttrs === "string") return this.styleAttr(type, { [nameOrAttrs]: val });
		let attrs = nameOrAttrs;
		if (this._tryRetarget(type, attrs)) return this;
		let morpher = new Morphable(this._stepper).to(attrs);
		let keys = Object.keys(attrs);
		this.queue(function() {
			morpher = morpher.from(this.element()[type](keys));
		}, function(pos) {
			this.element()[type](morpher.at(pos).valueOf());
			return morpher.done();
		}, function(newToAttrs) {
			const newKeys = Object.keys(newToAttrs);
			const differences = difference(newKeys, keys);
			if (differences.length) {
				const addedFromAttrs = this.element()[type](differences);
				const oldFromAttrs = new ObjectBag(morpher.from()).valueOf();
				Object.assign(oldFromAttrs, addedFromAttrs);
				morpher.from(oldFromAttrs);
			}
			const oldToAttrs = new ObjectBag(morpher.to()).valueOf();
			Object.assign(oldToAttrs, newToAttrs);
			morpher.to(oldToAttrs);
			keys = newKeys;
			attrs = newToAttrs;
		});
		this._rememberMorpher(type, morpher);
		return this;
	},
	zoom(level, point) {
		if (this._tryRetarget("zoom", level, point)) return this;
		let morpher = new Morphable(this._stepper).to(new SVGNumber(level));
		this.queue(function() {
			morpher = morpher.from(this.element().zoom());
		}, function(pos) {
			this.element().zoom(morpher.at(pos), point);
			return morpher.done();
		}, function(newLevel, newPoint) {
			point = newPoint;
			morpher.to(newLevel);
		});
		this._rememberMorpher("zoom", morpher);
		return this;
	},
	/**
	** absolute transformations
	**/
	transform(transforms, relative, affine) {
		relative = transforms.relative || relative;
		if (this._isDeclarative && !relative && this._tryRetarget("transform", transforms)) return this;
		const isMatrix = Matrix.isMatrixLike(transforms);
		affine = transforms.affine != null ? transforms.affine : affine != null ? affine : !isMatrix;
		const morpher = new Morphable(this._stepper).type(affine ? TransformBag : Matrix);
		let origin;
		let element;
		let current;
		let currentAngle;
		let startTransform;
		function setup() {
			element = element || this.element();
			origin = origin || getOrigin(transforms, element);
			startTransform = new Matrix(relative ? void 0 : element);
			element._addRunner(this);
			if (!relative) element._clearTransformRunnersBefore(this);
		}
		function run(pos) {
			if (!relative) this.clearTransform();
			const { x, y } = new Point(origin).transform(element._currentTransform(this));
			let target = new Matrix({
				...transforms,
				origin: [x, y]
			});
			let start = this._isDeclarative && current ? current : startTransform;
			if (affine) {
				target = target.decompose(x, y);
				start = start.decompose(x, y);
				const rTarget = target.rotate;
				const rCurrent = start.rotate;
				const possibilities = [
					rTarget - 360,
					rTarget,
					rTarget + 360
				];
				const distances = possibilities.map((a) => Math.abs(a - rCurrent));
				const shortest = Math.min(...distances);
				const index = distances.indexOf(shortest);
				target.rotate = possibilities[index];
			}
			if (relative) {
				if (!isMatrix) target.rotate = transforms.rotate || 0;
				if (this._isDeclarative && currentAngle) start.rotate = currentAngle;
			}
			morpher.from(start);
			morpher.to(target);
			const affineParameters = morpher.at(pos);
			currentAngle = affineParameters.rotate;
			current = new Matrix(affineParameters);
			this.addTransform(current);
			element._addRunner(this);
			return morpher.done();
		}
		function retarget(newTransforms) {
			if ((newTransforms.origin || "center").toString() !== (transforms.origin || "center").toString()) origin = getOrigin(newTransforms, element);
			transforms = {
				...newTransforms,
				origin
			};
		}
		this.queue(setup, run, retarget, true);
		this._isDeclarative && this._rememberMorpher("transform", morpher);
		return this;
	},
	x(x) {
		return this._queueNumber("x", x);
	},
	y(y) {
		return this._queueNumber("y", y);
	},
	ax(x) {
		return this._queueNumber("ax", x);
	},
	ay(y) {
		return this._queueNumber("ay", y);
	},
	dx(x = 0) {
		return this._queueNumberDelta("x", x);
	},
	dy(y = 0) {
		return this._queueNumberDelta("y", y);
	},
	dmove(x, y) {
		return this.dx(x).dy(y);
	},
	_queueNumberDelta(method, to) {
		to = new SVGNumber(to);
		if (this._tryRetarget(method, to)) return this;
		const morpher = new Morphable(this._stepper).to(to);
		let from = null;
		this.queue(function() {
			from = this.element()[method]();
			morpher.from(from);
			morpher.to(from + to);
		}, function(pos) {
			this.element()[method](morpher.at(pos));
			return morpher.done();
		}, function(newTo) {
			morpher.to(from + new SVGNumber(newTo));
		});
		this._rememberMorpher(method, morpher);
		return this;
	},
	_queueObject(method, to) {
		if (this._tryRetarget(method, to)) return this;
		const morpher = new Morphable(this._stepper).to(to);
		this.queue(function() {
			morpher.from(this.element()[method]());
		}, function(pos) {
			this.element()[method](morpher.at(pos));
			return morpher.done();
		});
		this._rememberMorpher(method, morpher);
		return this;
	},
	_queueNumber(method, value) {
		return this._queueObject(method, new SVGNumber(value));
	},
	cx(x) {
		return this._queueNumber("cx", x);
	},
	cy(y) {
		return this._queueNumber("cy", y);
	},
	move(x, y) {
		return this.x(x).y(y);
	},
	amove(x, y) {
		return this.ax(x).ay(y);
	},
	center(x, y) {
		return this.cx(x).cy(y);
	},
	size(width, height) {
		let box;
		if (!width || !height) box = this._element.bbox();
		if (!width) width = box.width / box.height * height;
		if (!height) height = box.height / box.width * width;
		return this.width(width).height(height);
	},
	width(width) {
		return this._queueNumber("width", width);
	},
	height(height) {
		return this._queueNumber("height", height);
	},
	plot(a, b, c, d) {
		if (arguments.length === 4) return this.plot([
			a,
			b,
			c,
			d
		]);
		if (this._tryRetarget("plot", a)) return this;
		const morpher = new Morphable(this._stepper).type(this._element.MorphArray).to(a);
		this.queue(function() {
			morpher.from(this._element.array());
		}, function(pos) {
			this._element.plot(morpher.at(pos));
			return morpher.done();
		});
		this._rememberMorpher("plot", morpher);
		return this;
	},
	leading(value) {
		return this._queueNumber("leading", value);
	},
	viewbox(x, y, width, height) {
		return this._queueObject("viewbox", new Box(x, y, width, height));
	},
	update(o) {
		if (typeof o !== "object") return this.update({
			offset: arguments[0],
			color: arguments[1],
			opacity: arguments[2]
		});
		if (o.opacity != null) this.attr("stop-opacity", o.opacity);
		if (o.color != null) this.attr("stop-color", o.color);
		if (o.offset != null) this.attr("offset", o.offset);
		return this;
	}
});
extend(Runner, {
	rx,
	ry,
	from,
	to
});
register(Runner, "Runner");
//#endregion
export { Runner as default };
