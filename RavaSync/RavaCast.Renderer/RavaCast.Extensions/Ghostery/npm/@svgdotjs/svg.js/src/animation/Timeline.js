import { registerMethods } from "../utils/methods.js";
import { globals } from "../utils/window.js";
import EventTarget from "../types/EventTarget.js";
import Animator from "./Animator.js";
//#region node_modules/@svgdotjs/svg.js/src/animation/Timeline.js
var makeSchedule = function(runnerInfo) {
	const start = runnerInfo.start;
	const duration = runnerInfo.runner.duration();
	return {
		start,
		duration,
		end: start + duration,
		runner: runnerInfo.runner
	};
};
var defaultSource = function() {
	const w = globals.window;
	return (w.performance || w.Date).now();
};
var Timeline = class extends EventTarget {
	constructor(timeSource = defaultSource) {
		super();
		this._timeSource = timeSource;
		this.terminate();
	}
	active() {
		return !!this._nextFrame;
	}
	finish() {
		this.time(this.getEndTimeOfTimeline() + 1);
		return this.pause();
	}
	getEndTime() {
		const lastRunnerInfo = this.getLastRunnerInfo();
		const lastDuration = lastRunnerInfo ? lastRunnerInfo.runner.duration() : 0;
		return (lastRunnerInfo ? lastRunnerInfo.start : this._time) + lastDuration;
	}
	getEndTimeOfTimeline() {
		const endTimes = this._runners.map((i) => i.start + i.runner.duration());
		return Math.max(0, ...endTimes);
	}
	getLastRunnerInfo() {
		return this.getRunnerInfoById(this._lastRunnerId);
	}
	getRunnerInfoById(id) {
		return this._runners[this._runnerIds.indexOf(id)] || null;
	}
	pause() {
		this._paused = true;
		return this._continue();
	}
	persist(dtOrForever) {
		if (dtOrForever == null) return this._persist;
		this._persist = dtOrForever;
		return this;
	}
	play() {
		this._paused = false;
		return this.updateTime()._continue();
	}
	reverse(yes) {
		const currentSpeed = this.speed();
		if (yes == null) return this.speed(-currentSpeed);
		const positive = Math.abs(currentSpeed);
		return this.speed(yes ? -positive : positive);
	}
	schedule(runner, delay, when) {
		if (runner == null) return this._runners.map(makeSchedule);
		let absoluteStartTime = 0;
		const endTime = this.getEndTime();
		delay = delay || 0;
		if (when == null || when === "last" || when === "after") absoluteStartTime = endTime;
		else if (when === "absolute" || when === "start") {
			absoluteStartTime = delay;
			delay = 0;
		} else if (when === "now") absoluteStartTime = this._time;
		else if (when === "relative") {
			const runnerInfo = this.getRunnerInfoById(runner.id);
			if (runnerInfo) {
				absoluteStartTime = runnerInfo.start + delay;
				delay = 0;
			}
		} else if (when === "with-last") {
			const lastRunnerInfo = this.getLastRunnerInfo();
			absoluteStartTime = lastRunnerInfo ? lastRunnerInfo.start : this._time;
		} else throw new Error("Invalid value for the \"when\" parameter");
		runner.unschedule();
		runner.timeline(this);
		const persist = runner.persist();
		const runnerInfo = {
			persist: persist === null ? this._persist : persist,
			start: absoluteStartTime + delay,
			runner
		};
		this._lastRunnerId = runner.id;
		this._runners.push(runnerInfo);
		this._runners.sort((a, b) => a.start - b.start);
		this._runnerIds = this._runners.map((info) => info.runner.id);
		this.updateTime()._continue();
		return this;
	}
	seek(dt) {
		return this.time(this._time + dt);
	}
	source(fn) {
		if (fn == null) return this._timeSource;
		this._timeSource = fn;
		return this;
	}
	speed(speed) {
		if (speed == null) return this._speed;
		this._speed = speed;
		return this;
	}
	stop() {
		this.time(0);
		return this.pause();
	}
	time(time) {
		if (time == null) return this._time;
		this._time = time;
		return this._continue(true);
	}
	unschedule(runner) {
		const index = this._runnerIds.indexOf(runner.id);
		if (index < 0) return this;
		this._runners.splice(index, 1);
		this._runnerIds.splice(index, 1);
		runner.timeline(null);
		return this;
	}
	updateTime() {
		if (!this.active()) this._lastSourceTime = this._timeSource();
		return this;
	}
	_continue(immediateStep = false) {
		Animator.cancelFrame(this._nextFrame);
		this._nextFrame = null;
		if (immediateStep) return this._stepImmediate();
		if (this._paused) return this;
		this._nextFrame = Animator.frame(this._step);
		return this;
	}
	_stepFn(immediateStep = false) {
		const time = this._timeSource();
		let dtSource = time - this._lastSourceTime;
		if (immediateStep) dtSource = 0;
		const dtTime = this._speed * dtSource + (this._time - this._lastStepTime);
		this._lastSourceTime = time;
		if (!immediateStep) {
			this._time += dtTime;
			this._time = this._time < 0 ? 0 : this._time;
		}
		this._lastStepTime = this._time;
		this.fire("time", this._time);
		for (let k = this._runners.length; k--;) {
			const runnerInfo = this._runners[k];
			const runner = runnerInfo.runner;
			if (this._time - runnerInfo.start <= 0) runner.reset();
		}
		let runnersLeft = false;
		for (let i = 0, len = this._runners.length; i < len; i++) {
			const runnerInfo = this._runners[i];
			const runner = runnerInfo.runner;
			let dt = dtTime;
			const dtToStart = this._time - runnerInfo.start;
			if (dtToStart <= 0) {
				runnersLeft = true;
				continue;
			} else if (dtToStart < dt) dt = dtToStart;
			if (!runner.active()) continue;
			if (!runner.step(dt).done) runnersLeft = true;
			else if (runnerInfo.persist !== true) {
				if (runner.duration() - runner.time() + this._time + runnerInfo.persist < this._time) {
					runner.unschedule();
					--i;
					--len;
				}
			}
		}
		if (runnersLeft && !(this._speed < 0 && this._time === 0) || this._runnerIds.length && this._speed < 0 && this._time > 0) this._continue();
		else {
			this.pause();
			this.fire("finished");
		}
		return this;
	}
	terminate() {
		this._startTime = 0;
		this._speed = 1;
		this._persist = 0;
		this._nextFrame = null;
		this._paused = true;
		this._runners = [];
		this._runnerIds = [];
		this._lastRunnerId = -1;
		this._time = 0;
		this._lastSourceTime = 0;
		this._lastStepTime = 0;
		this._step = this._stepFn.bind(this, false);
		this._stepImmediate = this._stepFn.bind(this, true);
	}
};
registerMethods({ Element: { timeline: function(timeline) {
	if (timeline == null) {
		this._timeline = this._timeline || new Timeline();
		return this._timeline;
	} else {
		this._timeline = timeline;
		return this;
	}
} } });
//#endregion
export { Timeline as default };
