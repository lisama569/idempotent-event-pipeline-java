package com.github.lisama569.idempotenteventpipelinejava.web;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.github.lisama569.idempotenteventpipelinejava.domain.ValidationException;

/** 接收阶段的分类拒绝：不同 RejectReason 给出稳定的错误码，结论可区分、不静默丢弃。 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Map<String, String>> handleValidation(ValidationException e) {
        log.warn("HTTP 拒绝 reason={} detail={}", e.getReason(), e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                "rejectReason", e.getReason().name(),
                "detail", e.getMessage() == null ? "" : e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleOther(Exception e) {
        log.warn("HTTP 未预期错误: {}", e.toString());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "rejectReason", "INTERNAL_ERROR",
                "detail", String.valueOf(e)));
    }
}
