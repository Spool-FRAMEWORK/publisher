package software.spool.janitor.internal.control;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.pipeline.PipelineContext;

import javax.management.AttributeNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QuarantineFailedEnvelopesStepTest {

    @Test
    void apply_quarantinedEnvelopes_updatesInboxStatus() throws AttributeNotFoundException {
        List<Collection<IdempotencyKey>> updated = new ArrayList<>();
        QuarantineFailedEnvelopesStep step = new QuarantineFailedEnvelopesStep(
            (keys, status) -> { updated.add(keys); return List.of(); },
            (value, attributes) -> {}
        );
        EnvelopeQuarantined event = EnvelopeQuarantined.builder()
            .idempotencyKey(IdempotencyKey.of("k1"))
            .violations(List.of("violation"))
            .build();
        PipelineContext ctx = PipelineContext.empty()
            .with(JanitorScheduleKeys.ENVELOPES_QUARANTINED, List.of(event));

        step.apply(ctx);

        assertThat(updated).hasSize(1);
        assertThat(updated.get(0)).containsExactly(IdempotencyKey.of("k1"));
    }

    @Test
    void apply_emptyList_updaterCalledWithEmptyKeys() throws AttributeNotFoundException {
        List<Collection<IdempotencyKey>> updated = new ArrayList<>();
        QuarantineFailedEnvelopesStep step = new QuarantineFailedEnvelopesStep(
            (keys, status) -> { updated.add(keys); return List.of(); },
            (value, attributes) -> {}
        );
        PipelineContext ctx = PipelineContext.empty()
            .with(JanitorScheduleKeys.ENVELOPES_QUARANTINED, List.of());

        step.apply(ctx);

        assertThat(updated).hasSize(1);
        assertThat(updated.get(0)).isEmpty();
    }
}
