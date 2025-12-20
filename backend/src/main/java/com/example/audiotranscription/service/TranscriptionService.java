package com.example.audiotranscription.service;

import java.util.Base64;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Flux;

@Service
public class TranscriptionService {
    private final WebClient client;
    private final ObjectMapper mapper = new ObjectMapper();
    // Limit concurrent in-flight transcription requests to avoid rate-limiting spikes
    // Increased to 2 to allow more concurrent requests while maintaining rate limiting
    private final java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(2);
    // Add delay between requests to further reduce rate limiting
    private volatile long lastRequestTime = 0;
    private static final long MIN_REQUEST_INTERVAL_MS = 60000; // 60 seconds between requests to reduce rate limiting

    public TranscriptionService(@Value("${google.speech.api.key}") String key,
                                @Value("${google.speech.api.url}") String url) {
        this.client = WebClient.builder().baseUrl(url + "?key=" + key).build();
    }

    public Flux<String> transcribe(byte[] audioData) {
        if (audioData == null || audioData.length < 100) return Flux.empty();

        // Debug: log request size
        System.out.println("[Transcribe] Received audio bytes: " + audioData.length);

        String base64Audio = Base64.getEncoder().encodeToString(audioData);

        Map<String, Object> body = Map.of(
            "contents", new Object[]{
                Map.of("parts", new Object[]{
                    // DATA FIRST, THEN TEXT PROMPT
                    Map.of("inline_data", Map.of("mime_type", "audio/wav", "data", base64Audio)),
                    Map.of("text", "Transcribe the audio. If there is no intelligible speech, return exactly the token [NO_TRANSCRIPTION]. Otherwise return ONLY the transcribed text. Keep it short and do not add extra commentary.")
                })
            }
        );

        // Try to acquire a permit (short wait) to avoid unlimited concurrent outbound requests
        return reactor.core.publisher.Mono.fromCallable(() -> {
                boolean acquired = false;
                try {
                    acquired = inFlight.tryAcquire(500, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                return acquired;
            })
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .flatMapMany(acquired -> {
                if (!acquired) {
                    System.err.println("[Transcribe] Server busy: too many concurrent transcriptions");
                    return Flux.just("[TRANSCRIPTION_BUSY]");
                }

                // Enforce minimum interval between requests to reduce rate limiting
                long now = System.currentTimeMillis();
                long timeSinceLast = now - lastRequestTime;
                if (timeSinceLast < MIN_REQUEST_INTERVAL_MS) {
                    long waitMs = MIN_REQUEST_INTERVAL_MS - timeSinceLast;
                    System.out.println("[Transcribe] Enforcing delay: waiting " + waitMs + "ms before request");
                    try {
                        Thread.sleep(waitMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return Flux.just("[TRANSCRIPTION_ERROR]");
                    }
                }
                lastRequestTime = System.currentTimeMillis();

                // perform the normal request but ensure permit is released when done
                return client.post()
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(body)
                    .exchangeToFlux(response -> {
                        int status = response.statusCode().value();
                        if (response.statusCode().is2xxSuccessful()) {
                            return response.bodyToFlux(String.class);
                        } else if (status == 429) {
                            String ra = response.headers().asHttpHeaders().getFirst("Retry-After");
                            final long retryAfter = parseRetryAfter(ra);
                            System.err.println("[Transcribe] Received 429 Too Many Requests; Retry-After=" + retryAfter);
                            return response.bodyToMono(String.class).defaultIfEmpty("").flatMapMany(b -> Flux.error(new TransientApiException("429", retryAfter)));
                        } else {
                            // log non-2xx body for debugging and surface a clear error
                            return response.bodyToMono(String.class).defaultIfEmpty("").flatMapMany(b -> {
                                String preview = b.length() > 1000 ? b.substring(0, 1000) : b;
                                System.err.println("[Transcribe] Non-2xx response: status=" + status + ", bodyPreview='" + preview + "'");
                                return Flux.error(new RuntimeException("Non-2xx response: " + status));
                            });
                        }
                    })
                    .doOnNext(s -> {
                        System.out.println("[Transcribe] Raw response chunk length=" + (s == null ? 0 : s.length()));
                        if (s != null && s.length() > 0 && s.length() < 2000) {
                            System.out.println("[Transcribe] Raw response (preview): " + s.substring(0, Math.min(1000, s.length())));
                        }
                    })
                    .map(this::extractText)
                    .doOnNext(t -> System.out.println("[Transcribe] Extracted text: '" + t + "'"))
                    .retryWhen(
                        reactor.util.retry.Retry.backoff(3, java.time.Duration.ofMillis(500))
                            .filter(throwable -> throwable instanceof TransientApiException)
                            .doBeforeRetry(rs -> {
                                Throwable f = rs.failure();
                                long wait = 0L;
                                if (f instanceof TransientApiException) wait = ((TransientApiException) f).getRetryAfterSeconds();
                                System.err.println("[Transcribe] Retry attempt " + (rs.totalRetriesInARow() + 1) + " (wait=" + wait + "s)");
                            })
                            .onRetryExhaustedThrow((spec, rs) -> rs.failure())
                    )
                    .doFinally(signal -> {
                        try {
                            inFlight.release();
                        } catch (Exception ignored) {}
                    })
                    .onErrorResume(err -> {
                        System.err.println("[Transcribe] Final failure after retries: " + err.getMessage());
                        err.printStackTrace();
                        return Flux.just("[TRANSCRIPTION_ERROR]");
                    });
            });
    }

    private static class TransientApiException extends RuntimeException {
        private final long retryAfterSeconds;
        public TransientApiException(String message, long retryAfterSeconds) {
            super(message);
            this.retryAfterSeconds = retryAfterSeconds;
        }
        public long getRetryAfterSeconds() { return retryAfterSeconds; }
    }

    private static long parseRetryAfter(String ra) {
        if (ra == null) return 0L;
        try {
            return Long.parseLong(ra);
        } catch (NumberFormatException nfe) {
            return 0L;
        }
    }

    private String extractText(String json) {
        try {
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(json);
            // Standard path for Gemini 2.0 Flash: candidates[0] -> content -> parts[0] -> text
            com.fasterxml.jackson.databind.JsonNode textNode = root.at("/candidates/0/content/parts/0/text");

            return textNode.isMissingNode() ? "" : textNode.asText().trim();
        } catch (Exception e) {
            System.err.println("JSON Parse Error: " + e.getMessage());
            return "";
        }
    }
}