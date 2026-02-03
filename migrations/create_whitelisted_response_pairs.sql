-- Whitelist for response pairs that should be included despite the >7d filter
CREATE TABLE IF NOT EXISTS whitelisted_response_pairs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email text NOT NULL,
    thread_id text NOT NULL,
    replied_at timestamptz NOT NULL,
    whitelisted_at timestamptz DEFAULT now(),
    UNIQUE (thread_id, replied_at)
);

CREATE INDEX IF NOT EXISTS idx_whitelisted_response_pairs_user
    ON whitelisted_response_pairs (user_email);

ALTER TABLE whitelisted_response_pairs DISABLE ROW LEVEL SECURITY;
