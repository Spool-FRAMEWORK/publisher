package software.spool.janitor.api.fixture;

import software.spool.core.exception.SpoolException;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.janitor.api.strategy.JanitorStrategy;

public class CapturingJanitorStrategy implements JanitorStrategy {
    private CancellationToken lastToken;
    private int executeCount;

    @Override
    public void execute(CancellationToken token) throws SpoolException {
        this.lastToken = token;
        this.executeCount++;
    }

    public CancellationToken lastToken() { return lastToken; }
    public boolean wasExecuted() { return lastToken != null; }
    public int executeCount() { return executeCount; }
}
