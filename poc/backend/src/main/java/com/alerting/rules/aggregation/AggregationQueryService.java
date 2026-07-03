package com.alerting.rules.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AggregationQueryService {

    /** Current event's named dimension values, set by CelEvaluator before each rule evaluation. */
    public static final ThreadLocal<Map<String, String>> CURRENT_DIMENSIONS = new ThreadLocal<>();

    private final JdbcTemplate clickHouseJdbcTemplate;

    public AggregationQueryService(@Qualifier("clickHouseJdbcTemplate") JdbcTemplate clickHouseJdbcTemplate) {
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
    }

    /**
     * Count events matching the given dimension scope in the last windowDays days.
     * Routes to the appropriate pre-aggregated rollup table for efficiency:
     * <ul>
     *   <li>≤ 1 day  → {@code events_*} (raw MergeTree, sub-day granularity)</li>
     *   <li>2–30 days → {@code mv_daily_*} (SummingMergeTree daily rollup)</li>
     *   <li>&gt; 30 days → {@code mv_monthly_*} (SummingMergeTree monthly rollup)</li>
     * </ul>
     *
     * @param sourceId   event source id, e.g. "external_bet"
     * @param dimSpec    pipe-separated dimension names, e.g. "punter_id" or "punter_id|bet_source"
     * @param windowDays look-back window in days
     */
    public long count(String sourceId, String dimSpec, long windowDays) {
        Map<String, String> dims = CURRENT_DIMENSIONS.get();
        String safeSourceId = sourceId.replaceAll("[^a-zA-Z0-9_]", "_");
        String sql;
        if (windowDays <= 1) {
            sql = String.format(
                    "SELECT count() FROM events_%s WHERE %s AND occurred_at >= now() - INTERVAL %d DAY",
                    safeSourceId, buildDimWhere(dimSpec), windowDays);
        } else if (windowDays <= 30) {
            // SummingMergeTree may have multiple rows per key before background merge; always use sum()
            sql = String.format(
                    "SELECT sum(event_count) FROM mv_daily_%s WHERE %s AND day >= today() - %d",
                    safeSourceId, buildDimWhere(dimSpec), windowDays);
        } else {
            sql = String.format(
                    "SELECT sum(event_count) FROM mv_monthly_%s WHERE %s AND month >= toStartOfMonth(today() - toIntervalDay(%d))",
                    safeSourceId, buildDimWhere(dimSpec), windowDays);
        }
        try {
            Long result = clickHouseJdbcTemplate.queryForObject(sql, Long.class, buildDimParams(dimSpec, dims));
            return result != null ? result : 0L;
        } catch (Exception e) {
            log.warn("agg_count failed for source={} dims={}: {}", sourceId, dimSpec, e.getMessage());
            return 0L;
        }
    }

    /**
     * Sum a numeric payload field for events matching the given dimension scope.
     *
     * @param field dot-notation payload path, e.g. "amount" or "betAmount.amount"
     */
    public double sum(String sourceId, String dimSpec, String field, long windowDays) {
        Map<String, String> dims = CURRENT_DIMENSIONS.get();
        String safeSourceId = sourceId.replaceAll("[^a-zA-Z0-9_]", "_");
        String sql = String.format(
                "SELECT sum(%s) FROM events_%s WHERE %s AND occurred_at >= now() - INTERVAL %d DAY",
                jsonExtractFloat(field), safeSourceId, buildDimWhere(dimSpec), windowDays);
        try {
            Double result = clickHouseJdbcTemplate.queryForObject(sql, Double.class, buildDimParams(dimSpec, dims));
            return result != null ? result : 0.0;
        } catch (Exception e) {
            log.warn("agg_sum failed for source={} dims={} field={}: {}", sourceId, dimSpec, field, e.getMessage());
            return 0.0;
        }
    }

    /**
     * Max of a numeric payload field for events matching the given dimension scope.
     */
    public double max(String sourceId, String dimSpec, String field, long windowDays) {
        Map<String, String> dims = CURRENT_DIMENSIONS.get();
        String safeSourceId = sourceId.replaceAll("[^a-zA-Z0-9_]", "_");
        String sql = String.format(
                "SELECT max(%s) FROM events_%s WHERE %s AND occurred_at >= now() - INTERVAL %d DAY",
                jsonExtractFloat(field), safeSourceId, buildDimWhere(dimSpec), windowDays);
        try {
            Double result = clickHouseJdbcTemplate.queryForObject(sql, Double.class, buildDimParams(dimSpec, dims));
            return result != null ? result : 0.0;
        } catch (Exception e) {
            log.warn("agg_max failed for source={} dims={} field={}: {}", sourceId, dimSpec, field, e.getMessage());
            return 0.0;
        }
    }

    /**
     * Lifetime sum from the pre-aggregated monthly rollup MV.
     */
    public double lifetimeSum(String sourceId, String dimSpec, String field) {
        Map<String, String> dims = CURRENT_DIMENSIONS.get();
        String safeSourceId = sourceId.replaceAll("[^a-zA-Z0-9_]", "_");
        String sql = String.format(
                "SELECT sum(total_amount) FROM mv_monthly_%s WHERE %s",
                safeSourceId, buildDimWhere(dimSpec));
        try {
            Double result = clickHouseJdbcTemplate.queryForObject(sql, Double.class, buildDimParams(dimSpec, dims));
            return result != null ? result : 0.0;
        } catch (Exception e) {
            log.warn("agg_lifetime_sum failed for source={}: {}", sourceId, e.getMessage());
            return 0.0;
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    /**
     * Builds a WHERE fragment for the given pipe-separated dimension spec.
     * "punter_id"              → "dim_punter_id = ?"
     * "punter_id|bet_source"   → "dim_punter_id = ? AND dim_bet_source = ?"
     */
    private static String buildDimWhere(String dimSpec) {
        return Arrays.stream(dimSpec.split("\\|"))
                .map(n -> "dim_" + n.trim().replaceAll("[^a-zA-Z0-9_]", "_") + " = ?")
                .collect(Collectors.joining(" AND "));
    }

    /**
     * Resolves the dimension values from the current event's dimension map,
     * in the same order as the names in dimSpec.
     */
    private static Object[] buildDimParams(String dimSpec, Map<String, String> dims) {
        return Arrays.stream(dimSpec.split("\\|"))
                .map(n -> dims != null ? dims.getOrDefault(n.trim(), "unknown") : "unknown")
                .toArray();
    }

    /**
     * Builds a ClickHouse JSONExtractFloat expression supporting one level of dot-notation.
     * "amount"           → JSONExtractFloat(payload, 'amount')
     * "betAmount.amount" → JSONExtractFloat(JSONExtractRaw(payload, 'betAmount'), 'amount')
     */
    private static String jsonExtractFloat(String fieldPath) {
        String[] parts = fieldPath.split("\\.", 2);
        String outer = parts[0].replaceAll("[^a-zA-Z0-9_]", "_");
        if (parts.length == 1) {
            return String.format("JSONExtractFloat(payload, '%s')", outer);
        }
        String inner = parts[1].replaceAll("[^a-zA-Z0-9_]", "_");
        return String.format("JSONExtractFloat(JSONExtractRaw(payload, '%s'), '%s')", outer, inner);
    }
}
