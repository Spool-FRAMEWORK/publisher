package software.spool.janitor.internal.control;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.inbox.InboxStatusQuery;
import software.spool.core.port.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class RepublishStuckEnvelopesStep implements Step<PipelineContext, PipelineContext> {
    private static final Logger LOG = LoggerFactory.getLogger(RepublishStuckEnvelopesStep.class);
    private final InboxStatusQuery reader;
    private final EventPublisher publisher;
    private final Duration threshold;

    public RepublishStuckEnvelopesStep(InboxStatusQuery reader, EventPublisher publisher, Duration threshold) {
        this.reader = reader;
        this.publisher = publisher;
        this.threshold = Objects.requireNonNullElse(threshold, Duration.ofMinutes(3));
    }

    @Override
    public PipelineContext apply(PipelineContext context) {
        reader.findByStatus(EnvelopeStatus.CAPTURED).stream()
                .filter(e -> getLastModifiedInstant(e).isBefore(Instant.now().minus(threshold)))
                .map(Envelope::retry)
                .peek(e -> LOG.warn("Republished Envelope {} | current attempt: {}", e, e.retries()))
                .map(this::buildEventFrom)
                .forEach(publisher::publish);
        return context;
    }

    private Instant getLastModifiedInstant(Envelope envelope) {
        return Objects.isNull(envelope.updatedAt()) ? envelope.capturedAt() : envelope.updatedAt();
    }

    private EnvelopeStored buildEventFrom(Envelope envelope) {
        return EnvelopeStored.builder()
                .correlationId(envelope.metadata().get(EventMetadataKey.CORRELATION_ID))
                .idempotencyKey(envelope.idempotencyKey())
                .build();
    }
}
