-- Create the excluded_response_pairs table for soft-delete exclusion
CREATE TABLE IF NOT EXISTS excluded_response_pairs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email text NOT NULL,
    external_sender text,
    subject text,
    received_at timestamptz,
    replied_at timestamptz,
    response_hours numeric,
    thread_id text,
    excluded_at timestamptz DEFAULT now(),
    UNIQUE (thread_id, replied_at)
);

-- Index for fast lookups when filtering response_pairs
CREATE INDEX IF NOT EXISTS idx_excluded_response_pairs_lookup
    ON excluded_response_pairs (thread_id, replied_at);

CREATE INDEX IF NOT EXISTS idx_excluded_response_pairs_user
    ON excluded_response_pairs (user_email);

-- Disable RLS so the service key can insert/delete without policies
ALTER TABLE excluded_response_pairs DISABLE ROW LEVEL SECURITY;
