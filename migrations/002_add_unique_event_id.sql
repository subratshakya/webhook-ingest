BEGIN;
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT unique_event_id UNIQUE (event_id);
COMMIT;
