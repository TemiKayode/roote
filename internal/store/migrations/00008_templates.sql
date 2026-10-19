-- +goose Up
CREATE TABLE templates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    template_key text NOT NULL,
    name text NOT NULL,
    active_version integer,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, template_key)
);

-- Versions are immutable once created; publishing points templates.active_version at one.
CREATE TABLE template_versions (
    template_id uuid NOT NULL REFERENCES templates(id) ON DELETE CASCADE,
    version integer NOT NULL CHECK (version > 0),
    default_locale text NOT NULL,
    contents jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (template_id, version)
);

-- +goose Down
DROP TABLE template_versions;
DROP TABLE templates;
