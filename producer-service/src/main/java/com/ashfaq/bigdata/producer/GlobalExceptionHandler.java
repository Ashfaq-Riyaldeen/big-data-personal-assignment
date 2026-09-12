package com.ashfaq.bigdata.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Turns failures into small, readable JSON responses.
 *
 * <p>Swagger UI shows the response body verbatim during the demonstration, so a field-by-field
 * validation message is far more useful on screen than a stack trace.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationFailure(
            MethodArgumentNotValidException exception) {

        Map<String, String> fieldErrors = new TreeMap<>();
        exception.getBindingResult().getFieldErrors()
                .forEach(error -> fieldErrors.put(error.getField(), error.getDefaultMessage()));

        log.warn("[REJECTED] invalid order request: {}", fieldErrors);

        return ResponseEntity.badRequest().body(body(HttpStatus.BAD_REQUEST,
                "The order request is not valid.", Map.of("fieldErrors", fieldErrors)));
    }

    @ExceptionHandler(OrderPublisher.PublishException.class)
    public ResponseEntity<Map<String, Object>> handlePublishFailure(
            OrderPublisher.PublishException exception) {

        log.error("[PUBLISH-FAILED] {}", exception.getMessage());

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(body(HttpStatus.SERVICE_UNAVAILABLE, exception.getMessage(), Map.of()));
    }

    private static Map<String, Object> body(HttpStatus status, String message,
                                            Map<String, Object> extra) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", status.getReasonPhrase());
        body.put("message", message);
        body.putAll(extra);
        return body;
    }
}
