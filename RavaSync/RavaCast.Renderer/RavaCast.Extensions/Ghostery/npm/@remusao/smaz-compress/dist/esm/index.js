import { create } from "../../../trie/dist/esm/index.js";
//#region node_modules/@remusao/smaz-compress/dist/esm/index.js
var EMPTY_UINT8_ARRAY = new Uint8Array(0);
var SmazCompress = class {
	constructor(codebook, maxSize = 3e4) {
		this.trie = create(codebook);
		this.buffer = new Uint8Array(maxSize);
		this.verbatim = new Uint8Array(255);
	}
	getCompressedSize(buffer) {
		if (buffer.length === 0) return 0;
		const retrieve = typeof buffer === "string" ? (idx) => buffer.charCodeAt(idx) : (idx) => buffer[idx];
		let bufferIndex = 0;
		let verbatimIndex = 0;
		let inputIndex = 0;
		while (inputIndex < buffer.length) {
			let indexAfterMatch = -1;
			let code = -1;
			let root = this.trie;
			for (let j = inputIndex; j < buffer.length; j += 1) {
				root = root.chars.get(retrieve(j));
				if (root === void 0) break;
				if (root.code !== void 0) {
					code = root.code;
					indexAfterMatch = j + 1;
				}
			}
			if (code === -1) {
				verbatimIndex++;
				inputIndex++;
				if (verbatimIndex === 255) {
					bufferIndex += 2 + verbatimIndex;
					verbatimIndex = 0;
				}
			} else {
				if (verbatimIndex !== 0) {
					bufferIndex += 2 + (verbatimIndex === 1 ? 0 : verbatimIndex);
					verbatimIndex = 0;
				}
				bufferIndex++;
				inputIndex = indexAfterMatch;
			}
		}
		if (verbatimIndex !== 0) bufferIndex += 2 + (verbatimIndex === 1 ? 0 : verbatimIndex);
		return bufferIndex;
	}
	compress(buffer) {
		if (buffer.length === 0) return EMPTY_UINT8_ARRAY;
		const retrieve = typeof buffer === "string" ? (idx) => buffer.charCodeAt(idx) : (idx) => buffer[idx];
		let bufferIndex = 0;
		let verbatimIndex = 0;
		let inputIndex = 0;
		while (inputIndex < buffer.length) {
			let indexAfterMatch = -1;
			let code = -1;
			let root = this.trie;
			for (let j = inputIndex; j < buffer.length; j += 1) {
				root = root.chars.get(retrieve(j));
				if (root === void 0) break;
				if (root.code !== void 0) {
					code = root.code;
					indexAfterMatch = j + 1;
				}
			}
			if (code === -1) {
				this.verbatim[verbatimIndex++] = retrieve(inputIndex++);
				if (verbatimIndex === 255) {
					bufferIndex = this.flushVerbatim(verbatimIndex, bufferIndex);
					verbatimIndex = 0;
				}
			} else {
				if (verbatimIndex !== 0) {
					bufferIndex = this.flushVerbatim(verbatimIndex, bufferIndex);
					verbatimIndex = 0;
				}
				this.buffer[bufferIndex++] = code;
				inputIndex = indexAfterMatch;
			}
		}
		if (verbatimIndex !== 0) bufferIndex = this.flushVerbatim(verbatimIndex, bufferIndex);
		return this.buffer.slice(0, bufferIndex);
	}
	flushVerbatim(verbatimIndex, bufferIndex) {
		if (verbatimIndex === 1) {
			this.buffer[bufferIndex++] = 254;
			this.buffer[bufferIndex++] = this.verbatim[0];
		} else {
			this.buffer[bufferIndex++] = 255;
			this.buffer[bufferIndex++] = verbatimIndex;
			for (let k = 0; k < verbatimIndex; k += 1) this.buffer[bufferIndex++] = this.verbatim[k];
		}
		return bufferIndex;
	}
};
//#endregion
export { SmazCompress };
