class AudioProcessor extends AudioWorkletProcessor {
    constructor() {
        super();
        this._bufferChunks = [];
        this._bufferLen = 0;
        this._gain = 5.0; // Amplify low volume
        // default target: 3.0 seconds (more context for STT models)
        this._targetFrames = Math.floor(sampleRate * 3.0);

        // Simple energy-based VAD parameters
        this._vadThreshold = 0.02; // RMS threshold after gain; tuned for common mics
        this._hangoverChunks = 2; // send up to this many extra chunks after speech
        this._hangoverLeft = 0;

        this.port.onmessage = (e) => {
            if (e.data && typeof e.data.setTargetSeconds === 'number') {
                this._targetFrames = Math.floor(sampleRate * e.data.setTargetSeconds);
            }
            if (e.data && typeof e.data.setVadThreshold === 'number') {
                this._vadThreshold = e.data.setVadThreshold;
            }
            if (e.data && typeof e.data.setHangoverChunks === 'number') {
                this._hangoverChunks = e.data.setHangoverChunks;
            }
        };
    }

    _rms(float32arr) {
        let s = 0.0;
        for (let i = 0; i < float32arr.length; i++) {
            const v = float32arr[i];
            s += v * v;
        }
        return Math.sqrt(s / float32arr.length);
    }

    process(inputs) {
        if (inputs[0] && inputs[0][0] && inputs[0][0].length > 0) {
            const input = inputs[0][0];
            const amplified = new Float32Array(input.length);
            for (let i = 0; i < input.length; i++) {
                amplified[i] = Math.max(-1, Math.min(1, input[i] * this._gain));
            }
            // buffer within the worklet
            this._bufferChunks.push(amplified);
            this._bufferLen += amplified.length;

            if (this._bufferLen >= this._targetFrames) {
                const out = new Float32Array(this._bufferLen);
                let off = 0;
                for (const c of this._bufferChunks) {
                    out.set(c, off);
                    off += c.length;
                }

                // compute RMS and decide whether to send or suppress
                const energy = this._rms(out);
                const voice = energy >= this._vadThreshold;
                if (voice) {
                    this._hangoverLeft = this._hangoverChunks;
                }

                if (voice || this._hangoverLeft > 0) {
                    // send transferable
                    this.port.postMessage(out, [out.buffer]);
                    this.port.postMessage({ type: 'vad', sent: true, energy: energy });
                    if (this._hangoverLeft > 0) this._hangoverLeft -= 1;
                } else {
                    // suppressed due to silence; send a small signal for UI/debug
                    this.port.postMessage({ type: 'vad', sent: false, energy: energy });
                }

                this._bufferChunks = [];
                this._bufferLen = 0;
            }
        }
        return true;
    }
}
registerProcessor("audio-processor", AudioProcessor);
