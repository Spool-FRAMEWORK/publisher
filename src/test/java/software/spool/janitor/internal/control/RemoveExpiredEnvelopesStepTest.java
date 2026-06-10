package software.spool.janitor.internal.control;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.vo.*;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.utils.routing.ErrorRouter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RemoveExpiredEnvelopesStepTest {

    @Test
    void apply_expiredEnvelopes_removedFromInbox() {
        List<IdempotencyKey> removed = new ArrayList<>();
        Envelope expired = anyEnvelope(Instant.parse("2000-01-01T00:00:00Z"));
        RemoveExpiredEnvelopesStep step = new RemoveExpiredEnvelopesStep(
            new ErrorRouter(),
            Duration.ofDays(365),
            keys -> { removed.addAll(keys); return List.of(); },
            status -> List.of(expired),
            (value, attributes) -> {}
        );

        step.apply(PipelineContext.empty());

        assertThat(removed).containsExactly(expired.idempotencyKey());
    }

    @Test
    void apply_freshEnvelopes_notRemoved() {
        List<IdempotencyKey> removed = new ArrayList<>();
        Envelope fresh = anyEnvelope(Instant.now());
        RemoveExpiredEnvelopesStep step = new RemoveExpiredEnvelopesStep(
            new ErrorRouter(),
            Duration.ofDays(365),
            keys -> { removed.addAll(keys); return List.of(); },
            status -> List.of(fresh),
            (value, attributes) -> {}
        );

        step.apply(PipelineContext.empty());

        assertThat(removed).isEmpty();
    }

    private static Envelope anyEnvelope(Instant updatedAt) {
        IdempotencyKey key = IdempotencyKey.of("k-" + updatedAt.toEpochMilli());
        return new Envelope(key, new EventMetadata(), MediaType.of("application/json"), "{}".getBytes(), EnvelopeStatus.PERSISTED, 0, updatedAt, updatedAt);
    }
}
