-- +goose Up
-- The accepting request's W3C traceparent, so the worker continues the same trace.
ALTER TABLE outbox ADD COLUMN trace_parent text;

-- +goose Down
ALTER TABLE outbox DROP COLUMN trace_parent;
