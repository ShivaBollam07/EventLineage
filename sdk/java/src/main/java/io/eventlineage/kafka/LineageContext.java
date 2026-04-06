package io.eventlineage.kafka;

/**
 * Thread-local lineage context for linking produced messages to consumed messages.
 *
 * When a message is consumed, the context is set with the consumed message's details.
 * Subsequent produces will automatically link to this parent message.
 */
public class LineageContext {

    private static final ThreadLocal<LineageContext> CURRENT = new ThreadLocal<>();

    private final String messageId;
    private final String correlationId;
    private final String rootMessageId;
    private final int retryCount;
    private String causationReason;

    public LineageContext(String messageId, String correlationId, String rootMessageId, int retryCount) {
        this.messageId = messageId;
        this.correlationId = correlationId;
        this.rootMessageId = rootMessageId;
        this.retryCount = retryCount;
    }

    /**
     * Get the current lineage context for this thread.
     */
    public static LineageContext current() {
        return CURRENT.get();
    }

    /**
     * Set the lineage context for this thread.
     */
    public static void set(LineageContext context) {
        CURRENT.set(context);
    }

    /**
     * Clear the lineage context for this thread.
     */
    public static void clear() {
        CURRENT.remove();
    }

    /**
     * Execute a runnable within a lineage context scope.
     */
    public static void runWithContext(LineageContext context, Runnable runnable) {
        LineageContext previous = current();
        try {
            set(context);
            runnable.run();
        } finally {
            if (previous != null) {
                set(previous);
            } else {
                clear();
            }
        }
    }

    public String getMessageId() {
        return messageId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getRootMessageId() {
        return rootMessageId;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public String getCausationReason() {
        return causationReason;
    }

    public void setCausationReason(String causationReason) {
        this.causationReason = causationReason;
    }

    /**
     * Get and clear the causation reason.
     */
    public String consumeCausationReason() {
        String reason = this.causationReason;
        this.causationReason = null;
        return reason;
    }

    @Override
    public String toString() {
        return "LineageContext{" +
                "messageId='" + messageId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", rootMessageId='" + rootMessageId + '\'' +
                ", retryCount=" + retryCount +
                '}';
    }
}
