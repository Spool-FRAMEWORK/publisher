package software.spool.janitor.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.port.health.HealthStatus;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.janitor.api.fixture.CapturingJanitorStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class JanitorTest {

    private CapturingJanitorStrategy strategy;
    private Janitor janitor;
    private SpoolNode.StartPermit permit;

    @BeforeEach
    void setUp() {
        strategy = new CapturingJanitorStrategy();
        janitor = new Janitor(strategy, new ErrorRouter(), ModuleHeartBeat.NOOP);
        permit = mock(SpoolNode.StartPermit.class);
    }

    @Test
    void start_delegatesToStrategy() {
        janitor.start(permit);
        assertThat(strategy.wasExecuted()).isTrue();
    }

    @Test
    void start_idempotent_doesNotStartTwice() {
        janitor.start(permit);
        janitor.start(permit);
        assertThat(strategy.executeCount()).isEqualTo(1);
    }

    @Test
    void stop_afterStart_cancelsPreviousToken() {
        janitor.start(permit);
        var lastToken = strategy.lastToken();

        janitor.stop(permit);

        assertThat(lastToken.isActive()).isFalse();
    }

    @Test
    void checkHealth_whenStopped_returnsDegraded() {
        assertThat(janitor.checkHealth().status()).isEqualTo(HealthStatus.DEGRADED);
    }

    @Test
    void checkHealth_whenRunning_returnsNotDegraded() {
        janitor.start(permit);
        assertThat(janitor.checkHealth().status()).isNotEqualTo(HealthStatus.DEGRADED);
    }
}
