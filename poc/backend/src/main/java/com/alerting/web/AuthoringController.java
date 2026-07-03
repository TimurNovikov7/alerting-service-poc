package com.alerting.web;

import com.alerting.authoring.BedrockLLMService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/authoring")
@RequiredArgsConstructor
public class AuthoringController {

    private final BedrockLLMService bedrockLLMService;

    /**
     * Generates a CEL expression from a natural-language description using AWS Bedrock.
     *
     * Request body: { "sourceId": "player_deposit", "description": "fire when deposit > 1000" }
     * Response:     { "celExpression": "payload.amount > 1000.0", "celSummary": "..." }
     */
    @PostMapping("/generate-cel")
    public ResponseEntity<Map<String, String>> generateCel(@RequestBody Map<String, String> body) {
        String sourceId = body.get("sourceId");
        String description = body.get("description");

        if (sourceId == null || sourceId.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "sourceId is required"));
        }
        if (description == null || description.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "description is required"));
        }

        try {
            Map<String, String> result = bedrockLLMService.generateCel(sourceId, description);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("CEL generation failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", e.getMessage()));
        }
    }
}
