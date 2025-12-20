package com.example.audiotranscription.config;

import com.example.audiotranscription.service.TranscriptionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TranscriptionWebSocketHandler extends BinaryWebSocketHandler {
    private final TranscriptionService transcriptionService;
    private final ObjectMapper mapper = new ObjectMapper();

    // Keep a small sliding-window of the last few received chunks per session to provide context if Gemini returns empty
    private final ConcurrentHashMap<String, Deque<byte[]>> sessionBuffers = new ConcurrentHashMap<>();

    // Active in-progress chunk assemblies (sessionId:chunkId -> baos)
    private final ConcurrentHashMap<String, java.io.ByteArrayOutputStream> assemblies = new ConcurrentHashMap<>();

    // Per-session pause (cooldown) state and failure counts to implement backoff
    private final ConcurrentHashMap<String, Long> sessionPausedUntil = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> sessionFailureCounts = new ConcurrentHashMap<>();

    public TranscriptionWebSocketHandler(TranscriptionService transcriptionService) {
        this.transcriptionService = transcriptionService;
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) {
        try {
            // 1. Extract audio bytes from the browser frame
            ByteBuffer payload = message.getPayload();
            byte[] audioData = new byte[payload.remaining()];
            payload.get(audioData);

            // If this session is paused due to recent failures, ignore processing and notify client
            Long pausedUntil = sessionPausedUntil.get(session.getId());
            long now = System.currentTimeMillis();
            if (pausedUntil != null && pausedUntil > now) {
                long ms = pausedUntil - now;
                try {
                    String pause = mapper.writeValueAsString(Map.of("type", "pause", "ms", ms));
                    session.sendMessage(new TextMessage(pause));
                    System.out.println("[WS] Ignoring binary frame for paused session " + session.getId() + " (ms=" + ms + ")");
                } catch (Exception e) {
                    System.err.println("Failed to send pause message to client: " + e.getMessage());
                }
                return;
            }

            // If there's an active assembly for this session, append and return
            // key is sessionId (we only allow one active assembly per session for simplicity)
            String keyPrefix = session.getId() + ":";
            java.util.Optional<String> activeKey = assemblies.keySet().stream().filter(k -> k.startsWith(keyPrefix)).findFirst();
            if (activeKey.isPresent()) {
                java.io.ByteArrayOutputStream baos = assemblies.get(activeKey.get());
                if (baos != null) {
                    baos.write(audioData);
                    System.out.println("[WS] Appended " + audioData.length + " bytes to assembly " + activeKey.get());
                }
                return; // don't process as standalone chunk
            }

            // Debug: log arrival and size
            System.out.println("[WS] handleBinaryMessage start for session " + session.getId() + ", bytes=" + audioData.length);

// Maintain a sliding per-session buffer (last N chunks)
        final int MERGE_WINDOW = 5; // more context if fallbacks are needed
        Deque<byte[]> dq = sessionBuffers.computeIfAbsent(session.getId(), k -> new ArrayDeque<>());
        dq.addLast(audioData);
        if (dq.size() > MERGE_WINDOW) dq.removeFirst();

        // Build a merged WAV from the last few chunks as a fallback (if short chunks produce empty transcription)
        byte[] merged = mergeWavChunks(new ArrayList<>(dq));
        boolean usedFallback = false;

            // 2. MERGED-FIRST: call transcribe on merged context first (more likely to contain intelligible speech), then fallback to single-chunk
            Flux<String> mergedFlux = transcriptionService.transcribe(merged)
                .doOnNext(t -> System.out.println("[WS] Merged candidate: '" + (t == null ? "" : t) + "'"));

            Flux<String> primary = transcriptionService.transcribe(audioData)
                .doOnNext(t -> System.out.println("[WS] Primary candidate: '" + (t == null ? "" : t) + "'"));

            // merged-first: if merged returns any non-empty, use it; otherwise use primary
            mergedFlux.collectList().flatMapMany(mList -> {
                boolean mergedHas = mList.stream().anyMatch(s -> s != null && !s.isBlank() && !s.equals("[NO_TRANSCRIPTION]"));
                if (mergedHas) {
                    System.out.println("[WS] Using merged-first results (window=" + MERGE_WINDOW + ")");
                    return Flux.fromIterable(mList);
                }
                System.out.println("[WS] Merged yielded no useful text; trying primary chunk");
                return primary;
            }).subscribe(
                text -> {
                    try {
                        if (session.isOpen()) {
                            // Reset failure counts / pause state on success
                            sessionFailureCounts.remove(session.getId());
                            sessionPausedUntil.remove(session.getId());

                            // Map special tokens to clearer client messages
                            String outText = text == null ? "" : text;
                            String type = "final";
                            if ("[TRANSCRIPTION_BUSY]".equals(outText)) {
                                type = "error";
                                outText = "Server busy; please slow down.";
                            } else if ("[TRANSCRIPTION_ERROR]".equals(outText)) {
                                type = "error";
                                outText = "Transcription failed; try again later.";
                            } else if (outText.isBlank() || outText.equals("[NO_TRANSCRIPTION]")) {
                                // keep final but normalize placeholder text
                                outText = "[No transcription]";
                            }
                            String payloadJson = mapper.writeValueAsString(Map.of("type", type, "text", outText));
                            System.out.println("[WS] Sending to session " + session.getId() + ": " + payloadJson);
                            session.sendMessage(new TextMessage(payloadJson));
                        }
                    } catch (Exception e) {
                        System.err.println("Error sending to UI: " + e.getMessage());
                        e.printStackTrace();
                    }
                },
                error -> {
                    System.err.println("Gemini Error: " + error.getMessage());
                    error.printStackTrace();
                    try {
                        if (session.isOpen()) {
                            String errMsg = mapper.writeValueAsString(Map.of("type", "error", "text", "Transient transcription error (rate-limited or API failure)."));
                            session.sendMessage(new TextMessage(errMsg));
                        }
                    } catch (Exception se) {
                        System.err.println("Failed to send error message to client: " + se.getMessage());
                        se.printStackTrace();
                    }

                    // apply a per-session cooldown/backoff
                    try {
                        setPauseForSession(session, 5000L);
                    } catch (Exception se) {
                        System.err.println("Failed to apply pause: " + se.getMessage());
                        se.printStackTrace();
                    }
                }
            );

            System.out.println("[WS] handleBinaryMessage done for session " + session.getId());
        } catch (Exception e) {
            System.err.println("Unhandled error in handleBinaryMessage for session " + session.getId() + ": " + e.getMessage());
            e.printStackTrace();
            try {
                if (session != null && session.isOpen()) {
                    String err = mapper.writeValueAsString(Map.of("type", "error", "text", "Server error: " + e.getMessage()));
                    session.sendMessage(new TextMessage(err));
                }
            } catch (Exception se) {
                System.err.println("Error sending error message to client: " + se.getMessage());
                se.printStackTrace();
            }
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, org.springframework.web.socket.TextMessage message) {
        String payload = message.getPayload();
        System.out.println("[WS] Received text message from " + session.getId() + ": " + payload);
        try {
            // try parse as JSON control message
            try {
                com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(payload);
                String type = node.has("type") ? node.get("type").asText() : null;
                if ("ping".equalsIgnoreCase(type) || "ping".equalsIgnoreCase(payload)) {
                    // Ping received, no response needed for transcription display
                    System.out.println("[WS] Ping received from session " + session.getId());
                    return;
                }

                if ("chunk-start".equals(type) && node.has("id")) {
                    // If this session is currently paused, inform client and ignore creating assembly
                    Long pausedUntil = sessionPausedUntil.get(session.getId());
                    long now = System.currentTimeMillis();
                    if (pausedUntil != null && pausedUntil > now) {
                        long ms = pausedUntil - now;
                        try {
                            String pause = mapper.writeValueAsString(Map.of("type", "pause", "ms", ms));
                            session.sendMessage(new TextMessage(pause));
                            System.out.println("[WS] Rejecting chunk-start for session " + session.getId() + " due to pause (ms=" + ms + ")");
                        } catch (Exception se) {
                            System.err.println("Failed to send pause message to client: " + se.getMessage());
                        }
                        return;
                    }

                    String id = node.get("id").asText();
                    String key = session.getId() + ":" + id;
                    assemblies.put(key, new java.io.ByteArrayOutputStream());
                    System.out.println("[WS] Started assembly " + key);
                    try {
                        String ack = mapper.writeValueAsString(Map.of("type", "ack", "id", id));
                        session.sendMessage(new TextMessage(ack));
                        System.out.println("[WS] Sent ack for " + key);
                    } catch (Exception se) {
                        System.err.println("Failed to send ack for " + key + ": " + se.getMessage());
                    }
                    return;
                }

                if ("chunk-end".equals(type) && node.has("id")) {
                    String id = node.get("id").asText();
                    String key = session.getId() + ":" + id;
                    java.io.ByteArrayOutputStream baos = assemblies.remove(key);
                    if (baos != null) {
                        byte[] full = baos.toByteArray();
                        System.out.println("[WS] Completed assembly " + key + " size=" + full.length);
                        try {
                            String ackEnd = mapper.writeValueAsString(Map.of("type", "ack-end", "id", id));
                            session.sendMessage(new TextMessage(ackEnd));
                            System.out.println("[WS] Sent ack-end for " + key);
                        } catch (Exception se) {
                            System.err.println("Failed to send ack-end for " + key + ": " + se.getMessage());
                        }
                        // process the reconstructed binary as a single chunk
                        handleReconstructedAudio(session, full);
                    } else {
                        System.err.println("[WS] Received chunk-end for unknown assembly " + key);
                    }
                    return;
                }

            } catch (Exception je) {
                // not JSON or control; fallthrough to legacy checks
            }

            // Removed ping response to avoid displaying "(echo) pong" in transcription
        } catch (Exception e) {
            System.err.println("Error handling text message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleReconstructedAudio(WebSocketSession session, byte[] audioData) {
        // Reuse existing flow but use the reconstructed audio as a single chunk
        try {
            System.out.println("[WS] Processing reconstructed chunk for session " + session.getId() + ", bytes=" + audioData.length);
            // Maintain sliding buffer
            Deque<byte[]> dq = sessionBuffers.computeIfAbsent(session.getId(), k -> new ArrayDeque<>());
            dq.addLast(audioData);
            if (dq.size() > 3) dq.removeFirst();
            byte[] merged = mergeWavChunks(new ArrayList<>(dq));

            Flux<String> primary = transcriptionService.transcribe(audioData);
            primary.collectList().flatMapMany(list -> {
                boolean hasNonEmpty = list.stream().anyMatch(s -> s != null && !s.isBlank());
                if (hasNonEmpty) return Flux.fromIterable(list);
                return transcriptionService.transcribe(merged).switchIfEmpty(Flux.fromIterable(list));
            }).subscribe(
                text -> {
                    try {
                        if (session.isOpen()) {
                            // Reset failure counts / pause state on success
                            sessionFailureCounts.remove(session.getId());
                            sessionPausedUntil.remove(session.getId());

                            String outText = text == null ? "" : text;
                            String type = "final";
                            if ("[TRANSCRIPTION_BUSY]".equals(outText)) {
                                type = "error";
                                outText = "Server busy; please slow down.";
                            } else if ("[TRANSCRIPTION_ERROR]".equals(outText)) {
                                type = "error";
                                outText = "Transcription failed; try again later.";
                            } else if (outText.isBlank() || outText.equals("[NO_TRANSCRIPTION]")) {
                                type = "partial"; // keep as partial while silence
                                outText = "";
                            }
                            String payloadJson = mapper.writeValueAsString(Map.of("type", type, "text", outText));
                            session.sendMessage(new TextMessage(payloadJson));
                        }
                    } catch (Exception e) {
                        System.err.println("Error sending to UI: " + e.getMessage());
                        e.printStackTrace();
                    }
                },
                error -> {
                    System.err.println("Gemini Error: " + error.getMessage());
                    error.printStackTrace();
                    try {
                        if (session.isOpen()) {
                            String errMsg = mapper.writeValueAsString(Map.of("type", "error", "text", "Transient transcription error (rate-limited or API failure)."));
                            session.sendMessage(new TextMessage(errMsg));
                        }
                    } catch (Exception se) {
                        System.err.println("Failed to send error message to client: " + se.getMessage());
                        se.printStackTrace();
                    }

                    // apply a per-session cooldown/backoff
                    try {
                        setPauseForSession(session, 5000L);
                    } catch (Exception se) {
                        System.err.println("Failed to apply pause: " + se.getMessage());
                        se.printStackTrace();
                    }
                }
            );
        } catch (Exception e) {
            System.err.println("Error processing reconstructed audio: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void setPauseForSession(WebSocketSession session, long baseMs) {
        try {
            String sid = session.getId();
            int attempts = sessionFailureCounts.getOrDefault(sid, 0) + 1;
            sessionFailureCounts.put(sid, attempts);
            long ms = baseMs * (1L << Math.max(0, attempts - 1));
            if (ms > 60000L) ms = 60000L; // cap at 60s
            long until = System.currentTimeMillis() + ms;
            sessionPausedUntil.put(sid, until);
            String pause = mapper.writeValueAsString(Map.of("type", "pause", "ms", ms));
            session.sendMessage(new TextMessage(pause));
            System.out.println("[WS] Paused session " + sid + " for " + ms + "ms (attempts=" + attempts + ")");
        } catch (Exception e) {
            System.err.println("Error setting pause for session: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Merge the data chunks of multiple WAV files into one WAV. This method extracts the 'data' subchunks and
     * concatenates them, updating a copy of the first header with the new sizes. If the input is empty, returns an empty byte[].
     */
    private static byte[] mergeWavChunks(List<byte[]> chunks) {
        if (chunks == null || chunks.isEmpty()) return new byte[0];
        List<byte[]> datas = new ArrayList<>();
        int total = 0;
        for (byte[] c : chunks) {
            int idx = indexOfDataSubchunk(c);
            if (idx < 0) continue;
            // 'data' subchunk size is at idx + 4
            int dataSize = readIntLE(c, idx + 4);
            int dataStart = idx + 8;
            if (dataStart + dataSize <= c.length) {
                byte[] d = new byte[dataSize];
                System.arraycopy(c, dataStart, d, 0, dataSize);
                datas.add(d);
                total += dataSize;
            }
        }
        if (total == 0) return new byte[0];
        byte[] first = chunks.get(0);
        byte[] out = new byte[44 + total];
        // copy header (first 44 bytes) then fix sizes
        System.arraycopy(first, 0, out, 0, 44);
        writeIntLE(out, 4, 36 + total); // overall file size - 8
        writeIntLE(out, 40, total); // data chunk size
        int pos = 44;
        for (byte[] d : datas) {
            System.arraycopy(d, 0, out, pos, d.length);
            pos += d.length;
        }
        return out;
    }

    private static int indexOfDataSubchunk(byte[] wav) {
        for (int i = 0; i < wav.length - 4; i++) {
            if (wav[i] == 'd' && wav[i + 1] == 'a' && wav[i + 2] == 't' && wav[i + 3] == 'a') return i;
        }
        return -1;
    }

    private static int readIntLE(byte[] b, int off) {
        return (b[off] & 0xff) | ((b[off + 1] & 0xff) << 8) | ((b[off + 2] & 0xff) << 16) | ((b[off + 3] & 0xff) << 24);
    }

    private static void writeIntLE(byte[] b, int off, int value) {
        b[off] = (byte) (value & 0xff);
        b[off + 1] = (byte) ((value >> 8) & 0xff);
        b[off + 2] = (byte) ((value >> 16) & 0xff);
        b[off + 3] = (byte) ((value >> 24) & 0xff);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, org.springframework.web.socket.CloseStatus status) throws Exception {
        System.out.println("[WS] Connection closed for " + session.getId() + " status=" + status);
        // cleanup per-session state
        sessionBuffers.remove(session.getId());
        assemblies.keySet().removeIf(k -> k.startsWith(session.getId() + ":"));
        sessionPausedUntil.remove(session.getId());
        sessionFailureCounts.remove(session.getId());
        super.afterConnectionClosed(session, status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        System.err.println("[WS] Transport error for session " + session.getId() + ": " + exception.getMessage());
        exception.printStackTrace();
        try {
            if (session.isOpen()) session.close(org.springframework.web.socket.CloseStatus.SERVER_ERROR);
        } catch (Exception ignored) { }
    }
}
