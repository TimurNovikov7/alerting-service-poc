package com.alerting.authoring;

import com.alerting.config.EventSourceProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.*;

import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class BedrockLLMService {

    private final EventSourceProperties eventSourceProperties;

    @Value("${aws.bedrock.model-id}")
    private String modelId;

    @Value("${aws.bedrock.region}")
    private String region;

    public Map<String, String> generateCel(String sourceId, String description) {
        EventSourceProperties.EventSource source = eventSourceProperties.getSources().stream()
                .filter(s -> s.getId().equals(sourceId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown source: " + sourceId));

        String fieldList = source.getPayloadFields().isEmpty()
                ? "amount (DOUBLE), ip (STRING), duration_seconds (DOUBLE)"
                : source.getPayloadFields().stream()
                        .map(f -> f.getName() + " (" + f.getType() + ")")
                        .collect(Collectors.joining(", "));

        String dimList = source.getEntity().getDimensions().stream()
                .map(EventSourceProperties.EntityConfig.DimensionConfig::getName)
                .collect(Collectors.joining(", "));

        String prompt = String.format("""
                You are a rule expression generator for an alerting system.

                Available event source: %s
                Payload fields available: %s

                Custom aggregation functions (values are scoped to the current event's dimension values):
                - agg_count("<sourceId>", "<dimSpec>", <windowDays>) → number of events in last N days
                    Single dim:   agg_count("punter_login", "punter_id", 7) — logins by this punter in 7 days
                    Compound dim: agg_count("external_bet", "punter_id|bet_source", 7) — bets by this punter from this same source in 7 days
                - agg_sum("<sourceId>", "<dimSpec>", "<field>", <windowDays>) → sum of a numeric payload field in last N days
                    Example: agg_sum("external_bet", "punter_id", "betAmount.amount", 30) — total staked by this punter in 30 days
                - agg_max("<sourceId>", "<dimSpec>", "<field>", <windowDays>) → max value of a numeric payload field in last N days
                    Example: agg_max("withdrawal", "punter_id", "amount", 7) — largest single withdrawal by this punter in 7 days

                Rules:
                - <sourceId> must be a string literal like "external_bet" — the exact source ID shown above
                - <dimSpec> must be a string literal — one dimension name OR multiple names joined with | for compound scope
                - <windowDays> must be a plain integer (1, 7, 30, 365 …) — never a string
                - agg_count takes exactly 3 arguments; agg_sum and agg_max take exactly 4
                - Available dimension names for this source: %s
                - IMPORTANT: dimension names (%s) are NOT accessible via payload.* — never reference them as payload fields
                - For count-based rules, agg_count alone is sufficient — no payload guard is needed

                Access current event payload fields as: payload.fieldName
                    Example: payload.amount > 500.0
                    Only these fields are valid: %s

                Compound expressions — use && (AND) and || (OR) to combine conditions:
                    payload.amount > 1000.0 && agg_count("withdrawal", "punter_id", 1) > 2
                    payload.currency == 'USD' || payload.currency == 'EUR'
                    agg_count("punter_login", "punter_id", 1) > 5 && agg_sum("external_bet", "punter_id", "betAmount.amount", 7) > 500.0
                    (payload.amount > 500.0 || agg_count("withdrawal", "punter_id", 7) > 3) && payload.currency == 'USD'
                - Use && / || freely; parentheses are supported
                - String equality: payload.currency == 'USD' (use single quotes for string literals)
                - For count-only rules, agg_count alone is sufficient — no extra payload guard needed

                Generate a boolean expression for the following alert rule:
                "%s"

                Respond ONLY with a JSON object in this exact format, no other text:
                {"celExpression": "<expression>", "celSummary": "<plain English summary>"}
                """, sourceId, fieldList, dimList, dimList, fieldList, description);

        try {
            BedrockRuntimeClient client = BedrockRuntimeClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                    .build();

            ConverseResponse response = client.converse(ConverseRequest.builder()
                    .modelId(modelId)
                    .messages(Message.builder()
                            .role(ConversationRole.USER)
                            .content(ContentBlock.fromText(prompt))
                            .build())
                    .build());

            String text = response.output().message().content().get(0).text();
            log.debug("Bedrock raw response: {}", text);

            int start = text.indexOf('{');
            int end = text.lastIndexOf('}') + 1;
            if (start >= 0 && end > start) {
                text = text.substring(start, end);
            }

            ObjectMapper mapper = new ObjectMapper();
            @SuppressWarnings("unchecked")
            Map<String, String> result = mapper.readValue(text, Map.class);
            return result;
        } catch (Exception e) {
            log.error("Bedrock LLM call failed: {}", e.getMessage(), e);
            throw new RuntimeException("LLM generation failed: " + e.getMessage(), e);
        }
    }
}
