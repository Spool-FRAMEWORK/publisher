-- 01_schema.sql
-- Inbox ultra-minimal

CREATE SCHEMA IF NOT EXISTS spool_inbox;

CREATE TABLE spool_inbox.events (
                                    id BIGSERIAL PRIMARY KEY,
                                    source VARCHAR(255),
                                    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                                    payload JSONB,
                                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices mínimos
CREATE INDEX idx_inbox_status ON spool_inbox.events(status);
CREATE INDEX idx_inbox_created ON spool_inbox.events(created_at);

-- Vista para eventos pendientes
CREATE VIEW spool_inbox.pending_events AS
SELECT * FROM spool_inbox.events
WHERE status = 'PENDING'
ORDER BY created_at ASC;

-- Función simple para marcar como DONE
CREATE OR REPLACE FUNCTION spool_inbox.mark_done(event_id BIGINT)
RETURNS VOID AS $$
BEGIN
UPDATE spool_inbox.events
SET status = 'DONE'
WHERE id = mark_done.event_id;
END;
$$ LANGUAGE plpgsql;

-- Función simple para marcar como FAILED
CREATE OR REPLACE FUNCTION spool_inbox.mark_failed(event_id BIGINT, new_status VARCHAR)
RETURNS VOID AS $$
BEGIN
UPDATE spool_inbox.events
SET status = new_status
WHERE id = mark_done.event_id;
END;
$$ LANGUAGE plpgsql;
