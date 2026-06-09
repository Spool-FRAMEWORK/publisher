package software.spool.janitor.internal.control;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.Event;
import software.spool.core.model.vo.*;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.port.bus.EventPublisher;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RepublishStuckEnvelopesStepTest {

    @Test
    void apply_stuckEnvelope_publishesEnvelopeStored() {
        List<Event> published = new ArrayList<>();
        Envelope stuck = anyEnvelope(0, Instant.parse("2000-01-01T00:00:00Z"));
        RepublishStuckEnvelopesStep step = new RepublishStuckEnvelopesStep(
            status -> List.of(stuck),
            (keys, status) -> List.of(),
            new EventPublisher() {
                @Override public <E extends Event> void publish(E event) { published.add(event); }
            },
            Duration.ZERO,
            3
        );

        step.apply(PipelineContext.empty());

        assertThat(published).hasSize(1);
    }

    @Test
    void apply_maxRetriesReached_quarantinesInstead() {
        List<Event> published = new ArrayList<>();
        List<IdempotencyKey> quarantined = new ArrayList<>();
        Envelope stuck = anyEnvelope(3, Instant.parse("2000-01-01T00:00:00Z"));
        RepublishStuckEnvelopesStep step = new RepublishStuckEnvelopesStep(
            status -> List.of(stuck),
            (keys, status) -> { if (status == EnvelopeStatus.QUARANTINED) quarantined.addAll(keys); return List.of(); },
            new EventPublisher() {
                @Override public <E extends Event> void publish(E event) { published.add(event); }
            },
            Duration.ZERO,
            3
        );

        step.apply(PipelineContext.empty());

        assertThat(published).isEmpty();
        assertThat(quarantined).hasSize(1);
    }

    private static Envelope anyEnvelope(int retries, Instant updatedAt) {
        IdempotencyKey key = IdempotencyKey.of("k-" + retries);
        return new Envelope(key, new EventMetadata(), MediaType.of("application/json"), "{}".getBytes(), EnvelopeStatus.CAPTURED, retries, updatedAt, updatedAt);
    }
}
