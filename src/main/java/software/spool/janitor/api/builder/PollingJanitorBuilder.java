package software.spool.janitor.api.builder;

import software.spool.core.pipeline.ObservedStep;
import software.spool.core.pipeline.Pipeline;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.EventSubscriber;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.decorator.SafeEventPublisher;
import software.spool.core.port.decorator.SafeEventSubscriber;
import software.spool.core.port.decorator.SafeInboxUpdater;
import software.spool.core.port.inbox.InboxEnvelopeRemover;
import software.spool.core.port.inbox.InboxStatusQuery;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.janitor.api.Janitor;
import software.spool.janitor.api.strategy.PollingJanitorStrategy;
import software.spool.janitor.api.utils.JanitorErrorRouter;
import software.spool.janitor.internal.control.*;
import software.spool.janitor.internal.port.decorator.SafeInboxStatusQuery;

import java.time.Duration;
import java.util.Objects;

public class PollingJanitorBuilder {
    private final ModuleHeartBeat heartBeat;
    private InboxStatusQuery reader;
    private InboxUpdater updater;
    private InboxEnvelopeRemover remover;
    private EventPublisher publisher;
    private EventSubscriber subscriber;
    private PollingConfiguration pollingConfiguration;
    private ErrorRouter errorRouter;
    private Integer millisecondsThreshold;
    private Integer millisecondsTtl;
    private Integer maxRetries;
    private final MetricsRegistry metricsRegistry = new OpenTelemetryMetricsRegistry();

    PollingJanitorBuilder(ModuleHeartBeat heartBeat) {
        this.heartBeat = heartBeat;
    }

    public PollingJanitorBuilder from(InboxStatusQuery reader) {
        this.reader = SafeInboxStatusQuery.of(reader);
        return this;
    }

    public PollingJanitorBuilder with(InboxUpdater updater) {
        this.updater = SafeInboxUpdater.of(updater);
        return this;
    }

    public PollingJanitorBuilder removeWith(InboxEnvelopeRemover remover) {
        this.remover = remover;
        return this;
    }

    public PollingJanitorBuilder on(EventPublisher publisher) {
        this.publisher = SafeEventPublisher.of(publisher);
        return this;
    }

    public PollingJanitorBuilder subscribeWith(EventSubscriber subscriber) {
        this.subscriber = SafeEventSubscriber.of(subscriber);
        return this;
    }

    public PollingJanitorBuilder every(Duration interval) {
        this.pollingConfiguration = PollingConfiguration.every(interval);
        return this;
    }

    public PollingJanitorBuilder withErrorRouter(ErrorRouter errorRouter) {
        this.errorRouter = errorRouter;
        return this;
    }

    public PollingJanitorBuilder withMillisecondsThreshold(Integer millisecondsThreshold) {
        this.millisecondsThreshold = millisecondsThreshold;
        return this;
    }

    public PollingJanitorBuilder withMillisecondsTtl(Integer millisecondsTtl) {
        this.millisecondsTtl = millisecondsTtl;
        return this;
    }

    public PollingJanitorBuilder withMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public Janitor create() {
        return new Janitor(initializeStrategy(), getErrorRouter(), heartBeat);
    }

    private ErrorRouter getErrorRouter() {
        return Objects.requireNonNullElse(errorRouter, JanitorErrorRouter.defaults(publisher));
    }

    private PollingJanitorStrategy initializeStrategy() {
        return new PollingJanitorStrategy(reader, subscriber, initializeJanitorScheduleHandler(), pollingConfiguration);
    }

    private Handler<EventsDTO> initializeJanitorScheduleHandler() {
        MetricsRegistry.CounterMetric cyclesCompleted = metricsRegistry.counter(SpoolMetrics.Janitor.CYCLES_COMPLETED_TOTAL, SpoolMetrics.Janitor.CYCLES_COMPLETED_TOTAL_DESC, "1");
        MetricsRegistry.CounterMetric cyclesFailed = metricsRegistry.counter(SpoolMetrics.Janitor.CYCLES_FAILED_TOTAL, SpoolMetrics.Janitor.CYCLES_FAILED_TOTAL_DESC, "1");
        MetricsRegistry.TimerMetric cycleDuration = metricsRegistry.timer(SpoolMetrics.Janitor.CYCLE_DURATION, SpoolMetrics.Janitor.CYCLE_DURATION_DESC, "s");
        MetricsRegistry.CounterMetric recordsCleaned = metricsRegistry.counter(SpoolMetrics.Janitor.RECORDS_CLEANED_TOTAL, SpoolMetrics.Janitor.RECORDS_CLEANED_TOTAL_DESC, "1");
        return new JanitorScheduleHandler(initializePipeline(recordsCleaned), getErrorRouter(), cyclesCompleted, cyclesFailed, cycleDuration);
    }

    private Pipeline<PipelineContext, PipelineContext> initializePipeline(MetricsRegistry.CounterMetric recordsCleaned) {
        return Pipeline.<PipelineContext>start()
                .add(new ObservedStep<>("update-persisted", new UpdatePersistedEnvelopesStep(updater)))
                .add(new ObservedStep<>("quarantine-envelopes", new QuarantineFailedEnvelopesStep(updater, recordsCleaned)))
                .add(new ObservedStep<>("expired-envelopes",
                        new RemoveExpiredEnvelopesStep(getErrorRouter(), Duration.ofMillis(millisecondsTtl), remover, reader, recordsCleaned)))
                .add(new ObservedStep<>("handle-stuck-envelopes",
                        new RepublishStuckEnvelopesStep(reader, updater, publisher, Duration.ofMillis(millisecondsThreshold), Objects.requireNonNullElse(maxRetries, 3))));
    }
}
