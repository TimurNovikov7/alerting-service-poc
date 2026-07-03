CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    source_id VARCHAR(100) NOT NULL,
    trigger_event_type VARCHAR(100),
    cel_expression TEXT NOT NULL,
    cel_summary TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    resolution_mode VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
    severity VARCHAR(20) NOT NULL DEFAULT 'MEDIUM',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id UUID NOT NULL REFERENCES rules(id),
    entity_dimension_value VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    severity VARCHAR(20) NOT NULL,
    fired_at TIMESTAMP NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    matched_event_snapshot TEXT,
    aggregation_snapshot TEXT
);

CREATE INDEX idx_alerts_dedup ON alerts(rule_id, entity_dimension_value, status)
    WHERE status IN ('OPEN', 'ACKNOWLEDGED');
CREATE INDEX idx_alerts_list ON alerts(status, fired_at DESC);
CREATE INDEX idx_alerts_rule ON alerts(rule_id, fired_at DESC);

-- Pre-seeded rules
-- agg_count("sourceId", "dimSpec", windowDays)
-- agg_sum("sourceId",  "dimSpec", "field", windowDays)
INSERT INTO rules (name, description, source_id, trigger_event_type,
                   cel_expression, cel_summary, enabled, resolution_mode, severity)
VALUES
    ('Large Withdrawal Alert',
     'Fires when a single withdrawal exceeds $500',
     'withdrawal', 'withdrawal',
     'payload.amount > 500.0',
     'Alert when withdrawal amount is greater than $500',
     TRUE, 'MANUAL', 'HIGH'),

    ('Suspicious Login Frequency',
     'Fires when the same punter logs in more than 3 times in the last day',
     'punter_login', 'LOGIN',
     'agg_count("punter_login", "punter_id", 1) > 3',
     'Alert when punter has more than 3 logins in the last day',
     TRUE, 'AUTO', 'MEDIUM'),

    ('High Daily Betting Volume',
     'Fires when total bet stake for the day exceeds $500',
     'external_bet', null,
     'agg_sum("external_bet", "punter_id", "betAmount.amount", 1) > 500.0',
     'Alert when total bet amount placed today exceeds $500',
     TRUE, 'AUTO', 'HIGH'),

    ('Concentrated Source Betting',
     'Fires when a punter places more than 20 bets from the same source in 7 days (compound dim)',
     'external_bet', null,
     'agg_count("external_bet", "punter_id|bet_source", 7) > 20',
     'Alert when punter places more than 20 bets from one specific source in 7 days',
     TRUE, 'AUTO', 'MEDIUM');
