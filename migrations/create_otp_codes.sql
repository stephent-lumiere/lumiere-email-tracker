-- Create the otp_codes table for the dashboard's email sign-in codes.
-- Codes are stored as an HMAC-SHA256 (keyed by OTP_SECRET), never in plain text.
CREATE TABLE IF NOT EXISTS otp_codes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email text NOT NULL,
    code_hash text NOT NULL,
    expires_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    consumed_at timestamptz
);

-- Index for the latest-code and rate-limit lookups
CREATE INDEX IF NOT EXISTS idx_otp_codes_email_created
    ON otp_codes (email, created_at DESC);

-- Enable RLS with no policies: anon/authenticated keys get no access, while the
-- service-role key the app uses bypasses RLS and keeps full access
ALTER TABLE otp_codes ENABLE ROW LEVEL SECURITY;
