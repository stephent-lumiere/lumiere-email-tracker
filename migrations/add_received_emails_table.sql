-- Migration: Create received_emails table
-- Run this migration in Supabase SQL Editor

-- 1. Create received_emails table to track all inbound external emails
CREATE TABLE IF NOT EXISTS received_emails (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email text NOT NULL,
    sender_email text NOT NULL,
    subject text,
    received_at timestamptz NOT NULL,
    thread_id text NOT NULL,
    replied boolean DEFAULT false,
    replied_at timestamptz,
    response_hours numeric,
    created_at timestamptz DEFAULT now(),
    UNIQUE (thread_id, received_at)
);

CREATE INDEX IF NOT EXISTS idx_received_emails_user ON received_emails (user_email);
CREATE INDEX IF NOT EXISTS idx_received_emails_thread ON received_emails (thread_id);

-- 2. Deactivate invalid user that cannot be impersonated via Google Workspace
UPDATE tracked_users
SET is_active = false
WHERE email = 'sagar.bhargava@wallstreetguide.net';
