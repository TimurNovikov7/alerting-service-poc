package com.alerting.ingestion.readiness;

public enum WaitMode {
    /**
     * Wait until ClickHouse's committed offset for the triggering partition exceeds the
     * event's own offset. Used for the source that produced the triggering event.
     */
    OFFSET_PAST,

    /**
     * Snapshot the high-watermark of a cross-referenced topic at evaluation start, then
     * wait until ClickHouse's committed offset reaches that watermark. Used for sources
     * referenced in CEL aggregation functions that were NOT the triggering source.
     */
    END_OFFSET
}
