package com.alerting.rules.engine;

import com.alerting.rules.aggregation.AggregationQueryService;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class CelEvaluator {

    private static final Logger log = LoggerFactory.getLogger(CelEvaluator.class);

    private final AggregationQueryService aggregationQueryService;
    private final CelCompiler celCompiler;
    private final CelRuntime celRuntime;

    /**
     * Compiled CEL programs keyed by expression string.
     * Compilation (parse + type-check) is the expensive step; evaluation of a cached Program is ~microseconds.
     */
    private final ConcurrentHashMap<String, CelRuntime.Program> programCache = new ConcurrentHashMap<>();

    /** Used only in buildAggregationSnapshot — not on the evaluation hot path. */
    private static final Pattern AGG_PATTERN = Pattern.compile("agg_\\w+\\([^)]+\\)");

    public CelEvaluator(AggregationQueryService aggregationQueryService) {
        this.aggregationQueryService = aggregationQueryService;

        // Compiler — declares payload variable type and custom function signatures for type-checking
        this.celCompiler = CelCompilerFactory.standardCelCompilerBuilder()
                // payload: map<string, dyn> — payload.fieldName maps to payload["fieldName"] in CEL
                .addVar("payload", MapType.create(SimpleType.STRING, SimpleType.DYN))
                .addFunctionDeclarations(
                        // agg_count("sourceId", "dimSpec", windowDays) → int
                        CelFunctionDecl.newFunctionDeclaration("agg_count",
                                CelOverloadDecl.newGlobalOverload("agg_count_3", SimpleType.INT,
                                        SimpleType.STRING, SimpleType.STRING, SimpleType.INT)),
                        // agg_sum("sourceId", "dimSpec", "field", windowDays) → double
                        CelFunctionDecl.newFunctionDeclaration("agg_sum",
                                CelOverloadDecl.newGlobalOverload("agg_sum_4", SimpleType.DOUBLE,
                                        SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.INT)),
                        // agg_max("sourceId", "dimSpec", "field", windowDays) → double
                        CelFunctionDecl.newFunctionDeclaration("agg_max",
                                CelOverloadDecl.newGlobalOverload("agg_max_4", SimpleType.DOUBLE,
                                        SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.INT)),
                        // agg_lifetime_sum("sourceId", "dimSpec", "field") → double
                        CelFunctionDecl.newFunctionDeclaration("agg_lifetime_sum",
                                CelOverloadDecl.newGlobalOverload("agg_lifetime_sum_3", SimpleType.DOUBLE,
                                        SimpleType.STRING, SimpleType.STRING, SimpleType.STRING))
                )
                .build();

        // Runtime — binds overload IDs to Java lambda implementations
        this.celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder()
                .addFunctionBindings(
                        CelRuntime.CelFunctionBinding.from("agg_count_3",
                                List.of(String.class, String.class, Long.class),
                                args -> aggregationQueryService.count(
                                        (String) args[0], (String) args[1], (Long) args[2])),

                        CelRuntime.CelFunctionBinding.from("agg_sum_4",
                                List.of(String.class, String.class, String.class, Long.class),
                                args -> aggregationQueryService.sum(
                                        (String) args[0], (String) args[1],
                                        (String) args[2], (Long) args[3])),

                        CelRuntime.CelFunctionBinding.from("agg_max_4",
                                List.of(String.class, String.class, String.class, Long.class),
                                args -> aggregationQueryService.max(
                                        (String) args[0], (String) args[1],
                                        (String) args[2], (Long) args[3])),

                        CelRuntime.CelFunctionBinding.from("agg_lifetime_sum_3",
                                List.of(String.class, String.class, String.class),
                                args -> aggregationQueryService.lifetimeSum(
                                        (String) args[0], (String) args[1], (String) args[2]))
                )
                .build();
    }

    /**
     * Evaluates a CEL boolean expression against the event payload.
     * Supports: payload field checks, agg_* calls, &&, ||, parentheses, comparisons.
     * Examples:
     *   payload.amount > 500.0
     *   agg_count("punter_login", "punter_id", 1) > 3
     *   payload.amount > 500.0 && agg_count("withdrawal", "punter_id", 7) > 2
     *   (payload.amount > 1000.0 || agg_count(...) > 5) && payload.currency == 'USD'
     *
     * @param expression  CEL boolean expression
     * @param payload     flat map of payload fields for this event
     * @param dimensions  named dimension values, e.g. {"punter_id":"123","bet_source":"mobile"}
     */
    public boolean evaluate(String expression, Map<String, Object> payload, Map<String, String> dimensions) {
        AggregationQueryService.CURRENT_DIMENSIONS.set(dimensions);
        try {
            CelRuntime.Program program = getOrCompile(expression);
            Object result = program.eval(Map.of("payload", payload != null ? payload : Map.of()));
            return Boolean.TRUE.equals(result);
        } catch (CelEvaluationException e) {
            log.warn("CEL evaluation error for '{}': {}", expression, e.getMessage());
            return false;
        } catch (Exception e) {
            log.warn("Unexpected error evaluating '{}': {}", expression, e.getMessage());
            return false;
        } finally {
            AggregationQueryService.CURRENT_DIMENSIONS.remove();
        }
    }

    /**
     * Builds a snapshot of all aggregation values referenced in the expression.
     * Called only when an alert fires — not on the hot path.
     */
    public Map<String, Object> buildAggregationSnapshot(String expression, Map<String, String> dimensions) {
        AggregationQueryService.CURRENT_DIMENSIONS.set(dimensions);
        Map<String, Object> snapshot = new HashMap<>();
        try {
            Matcher matcher = AGG_PATTERN.matcher(expression);
            while (matcher.find()) {
                String call = matcher.group();
                try {
                    snapshot.put(call, evaluateAggCallDirect(call));
                } catch (Exception e) {
                    log.debug("Snapshot capture failed for {}: {}", call, e.getMessage());
                }
            }
        } finally {
            AggregationQueryService.CURRENT_DIMENSIONS.remove();
        }
        return snapshot;
    }

    // ── private helpers ────────────────────────────────────────────────────────────────────────

    private CelRuntime.Program getOrCompile(String expression) {
        return programCache.computeIfAbsent(expression, expr -> {
            try {
                CelAbstractSyntaxTree ast = celCompiler.compile(expr).getAst();
                return celRuntime.createProgram(ast);
            } catch (CelValidationException e) {
                throw new IllegalArgumentException(
                        "CEL compilation failed for '" + expr + "': " + e.getMessage(), e);
            } catch (CelEvaluationException e) {
                throw new IllegalArgumentException(
                        "CEL program creation failed for '" + expr + "': " + e.getMessage(), e);
            }
        });
    }

    /** Direct aggregation dispatch used only for alert snapshot capture. */
    private Object evaluateAggCallDirect(String call) {
        int paren = call.indexOf('(');
        String funcName = call.substring(0, paren);
        String[] args = parseArgs(call.substring(paren + 1, call.length() - 1));
        return switch (funcName) {
            case "agg_count"        -> aggregationQueryService.count(args[0], args[1], Long.parseLong(args[2]));
            case "agg_sum"          -> aggregationQueryService.sum(args[0], args[1], args[2], Long.parseLong(args[3]));
            case "agg_max"          -> aggregationQueryService.max(args[0], args[1], args[2], Long.parseLong(args[3]));
            case "agg_lifetime_sum" -> aggregationQueryService.lifetimeSum(args[0], args[1], args[2]);
            default                 -> 0;
        };
    }

    private static String[] parseArgs(String argsStr) {
        String[] raw = argsStr.split(",");
        String[] result = new String[raw.length];
        for (int i = 0; i < raw.length; i++) {
            result[i] = raw[i].trim().replace("\"", "").replace("'", "");
        }
        return result;
    }
}
