const canvas = document.getElementById("equalizerCanvas");
const ctx = canvas.getContext("2d");
const startBtn = document.getElementById("startBtn");
const stopBtn = document.getElementById("stopBtn");
const transcriptionText = document.getElementById("transcriptionText");
const wsStatus = document.getElementById("wsState");

let audioContext, analyser, websocket, animationId, running = false, buffer = [], reconnectAttempts = 0, maxReconnectAttempts = 5;
let sendPausedUntil = 0; // ms timestamp until which sending is paused (set by server)
let pingIntervalId = null; // id for periodic client-side ping to keep connection alive (ms)

const RATE = 16000;
const CHUNK_TIME = 10.0;
const BARS = 256;
const CX = 200, CY = 200, R = 150;

startBtn.onclick = start;
stopBtn.onclick = stop;

async function start() {
    if (running) return;
    running = true;
    try {
        const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
        audioContext = new AudioContext({ sampleRate: RATE });
        const SR = audioContext.sampleRate;
        console.log("AudioContext sampleRate=", SR);
        const source = audioContext.createMediaStreamSource(stream);
        analyser = audioContext.createAnalyser();
        analyser.fftSize = 512;
        source.connect(analyser);

        // Use a managed WebSocket with send queue + reconnect logic
        let sendQueue = [];
        const SLICE_SIZE = 8000; // send binary slices <= 8KB to avoid server buffer limits

        const pendingAcks = new Map();
        function waitForAck(id, timeoutMs = 1000) {
            return new Promise((resolve, reject) => {
                pendingAcks.set(id, resolve);
                setTimeout(() => {
                    if (pendingAcks.has(id)) {
                        pendingAcks.delete(id);
                        reject(new Error('Ack timeout'));
                    }
                }, timeoutMs);
            });
        }

        async function sendBlobAsSlices(blob) {
            const id = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
            try {
                console.log('sendBlobAsSlices: start id=', id, 'size=', blob.size);

                // send chunk-start and wait for ack (best-effort with timeout)
                try {
                    websocket.send(JSON.stringify({ type: 'chunk-start', id }));
                    await waitForAck(id, 1000);
                    console.log('sendBlobAsSlices: received ack for id=', id);
                } catch (ackErr) {
                    console.warn('No ack received for', id, '- sending slices anyway');
                }

                let offset = 0;
                let sliceIndex = 0;
                while (offset < blob.size) {
                    const end = Math.min(offset + SLICE_SIZE, blob.size);
                    const slice = blob.slice(offset, end);
                    websocket.send(slice);
                    console.log('sendBlobAsSlices: sent slice', sliceIndex, 'bytes=', slice.size);
                    offset = end;
                    sliceIndex++;
                }
                websocket.send(JSON.stringify({ type: 'chunk-end', id }));
                console.log('sendBlobAsSlices: end id=', id);
            } catch (e) {
                console.error('Error sending slices', e);
                // re-queue full blob on error
                sendQueue.push(blob);
            }
        }

        function sendOrQueue(blob) {
            const now = Date.now();
            if (sendPausedUntil && now < sendPausedUntil) {
                // respect server pause: queue the blob and log
                sendQueue.push(blob);
                console.warn("Send paused by server; queued audio chunk, frames=", blob.size || '(blob)', "resumeInMs=", (sendPausedUntil - now));
                return;
            }
            if (websocket && websocket.readyState === WebSocket.OPEN) {
                sendBlobAsSlices(blob);
            } else {
                sendQueue.push(blob);
                console.warn("WebSocket not open; queued audio chunk, frames=", blob.size || '(blob)');
            }
        }

        function flushQueue() {
            while (sendQueue.length > 0 && websocket && websocket.readyState === WebSocket.OPEN) {
                const b = sendQueue.shift();
                console.log("Flushing queued audio chunk, size=", b.size || '(blob)');
                sendBlobAsSlices(b);
            }
        }

        function createWebSocket() {
            websocket = new WebSocket("ws://localhost:9090/transcription");
            websocket.binaryType = 'blob';

            websocket.onopen = () => {
                console.log("WebSocket opened");
                wsStatus.textContent = "connected";
                wsStatus.style.color = "green";
                reconnectAttempts = 0;
                // flush any queued audio chunks
                flushQueue();
                // send an initial ping and start periodic pings to keep connections alive
                try { websocket.send('ping'); } catch (e) { }
                try {
                    if (pingIntervalId) clearInterval(pingIntervalId);
                    pingIntervalId = setInterval(() => {
                        try { if (websocket && websocket.readyState === WebSocket.OPEN) websocket.send('ping'); } catch (e) { }
                    }, 15000);
                } catch (e) { }
            };

            websocket.onmessage = e => {
                console.log("Raw response from backend:", e.data);
                let msg = null;
                try {
                    msg = JSON.parse(e.data);
                } catch (err) {
                    msg = null;
                }

                // Handle ack messages first
                if (msg && msg.type === 'ack' && msg.id) {
                    const resolver = pendingAcks.get(msg.id);
                    if (resolver) {
                        resolver();
                        pendingAcks.delete(msg.id);
                        console.log('Received ack for id=', msg.id);
                    }
                    return;
                }

                // ack-end (assembly completed)
                if (msg && msg.type === 'ack-end' && msg.id) {
                    console.log('Received ack-end for id=', msg.id);
                    return;
                }

                if (msg && msg.type) {
                    if (msg.type === 'partial') {
                        const partialEl = document.getElementById('partialText');
                        partialEl.textContent = msg.text || "…";
                    } else if (msg.type === 'final') {
                        // If server returned the special token for silence or empty, show a user-friendly placeholder
                        if (msg.text === '[NO_TRANSCRIPTION]' || !(msg.text && msg.text.trim())) {
                            transcriptionText.innerText += "[No transcription] ";
                            transcriptionText.scrollTop = transcriptionText.scrollHeight;
                        } else {
                            transcriptionText.innerText += msg.text;
                            transcriptionText.scrollTop = transcriptionText.scrollHeight;
                        }
                        document.getElementById('partialText').textContent = "";
                    } else if (msg.type === 'error') {
                        transcriptionText.innerText += msg.text + " ";
                        transcriptionText.scrollTop = transcriptionText.scrollHeight;
                    } else if (msg.type === 'pause') {
                        // server asked us to pause sending for a duration
                        try {
                            const ms = Number(msg.ms) || 0;
                            sendPausedUntil = Date.now() + ms;
                            console.warn('Server pause received: ms=', ms);
                            // ensure queued chunks are flushed after the pause
                            setTimeout(() => {
                                sendPausedUntil = 0;
                                console.log('Server pause expired; flushing queue');
                                flushQueue();
                            }, ms);
                        } catch (e) {
                            console.warn('Invalid pause message from server', msg);
                        }
                    }
                } else {
                    if (e.data && e.data.trim()) {
                        transcriptionText.innerText += e.data + " ";
                        transcriptionText.scrollTop = transcriptionText.scrollHeight;
                    }
                }
            };

            websocket.onclose = (event) => {
                wsStatus.textContent = "disconnected";
                wsStatus.style.color = "red";
                console.warn("WebSocket closed. Attempting reconnect... code=", event.code, "reason=", event.reason);
                // clear ping timer
                try { if (pingIntervalId) { clearInterval(pingIntervalId); pingIntervalId = null; } } catch (e) {}
                if (reconnectAttempts < maxReconnectAttempts) {
                    const backoff = Math.min(10000, 1000 * Math.pow(2, reconnectAttempts));
                    reconnectAttempts++;
                    setTimeout(() => createWebSocket(), backoff);
                } else {
                    console.error("Max reconnect attempts reached");
                }
            };

            websocket.onerror = (err) => {
                console.error("WebSocket error", err);
            };
        }

        // create the socket (will flush queued chunks when opened)
        createWebSocket();
        await audioContext.audioWorklet.addModule("audio-processor.js");
        const processor = new AudioWorkletNode(audioContext, "audio-processor");

        // Request worklet to produce ~15.0 second chunks (adjustable) to further reduce API rate limiting
        processor.port.postMessage({ setTargetSeconds: 15.0 });

        processor.port.onmessage = async (event) => {
            const data = event.data;
            // VAD control messages from worklet
            if (data && data.type === 'vad') {
                if (!data.sent) {
                    console.log("VAD: suppressed chunk (energy=" + data.energy.toFixed(5) + ")");
                } else {
                    console.log("VAD: sent chunk (energy=" + data.energy.toFixed(5) + ")");
                }
                return;
            }

            let samples = data;
            if (!(samples instanceof Float32Array)) {
                samples = new Float32Array(samples);
            }

            const wavBuffer = await toWav(samples, SR);
            const blob = new Blob([wavBuffer], { type: 'audio/wav' });
            console.log("Prepared audio blob (frames=" + samples.length + ", sr=" + SR + ")");
            sendOrQueue(blob);
        };
        
        source.connect(processor);
        startBtn.disabled = true;
        stopBtn.disabled = false;
        draw();
    } catch (err) {
        running = false;
        alert("Microphone access denied. Run on http://localhost:3000 and allow access.");
    }
}

function stop() {
    running = false;
    if (audioContext) audioContext.close();
    if (websocket) websocket.close();
    startBtn.disabled = false;
    stopBtn.disabled = true;
    cancelAnimationFrame(animationId);
    ctx.clearRect(0, 0, 400, 400);
}

function draw() {
    if (!running) return;
    const data = new Uint8Array(analyser.frequencyBinCount);
    analyser.getByteFrequencyData(data);
    ctx.clearRect(0, 0, 400, 400);
    const step = (Math.PI * 2) / BARS;
    for (let i = 0; i < BARS; i++) {
        const h = (data[i] / 255) * 100;
        const a = i * step;
        ctx.beginPath();
        ctx.moveTo(CX + Math.cos(a) * R, CY + Math.sin(a) * R);
        ctx.lineTo(CX + Math.cos(a) * (R + h), CY + Math.sin(a) * (R + h));
        ctx.strokeStyle = `hsl(${(i / BARS) * 360},100%, 50%)`;
        ctx.lineWidth = 3;
        ctx.stroke();
    }
    animationId = requestAnimationFrame(draw);
}

async function toWav(samples, rate) {
    const buf = new ArrayBuffer(44 + samples.length * 2);
    const view = new DataView(buf);
    const s = (o, v) => { for (let i = 0; i < v.length; i++) view.setUint8(o + i, v.charCodeAt(i)); };
    s(0, "RIFF"); view.setUint32(4, 36 + samples.length * 2, true);
    s(8, "WAVE"); s(12, "fmt "); view.setUint32(16, 16, true);
    view.setUint16(20, 1, true); view.setUint16(22, 1, true);
    view.setUint32(24, rate, true); view.setUint32(28, rate * 2, true);
    view.setUint16(32, 2, true); view.setUint16(34, 16, true);
    s(36, "data"); view.setUint32(40, samples.length * 2, true);
    for (let i = 0, o = 44; i < samples.length; i++, o += 2) {
        let v = Math.max(-1, Math.min(1, samples[i]));
        view.setInt16(o, v < 0 ? v * 0x8000 : v * 0x7fff, true);
    }
    return buf;
}