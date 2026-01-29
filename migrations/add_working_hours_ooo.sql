-- Migration: Add working hours and out-of-office tracking
-- Run this migration in Supabase SQL Editor

-- 1. Add working hours columns to tracked_users
ALTER TABLE tracked_users ADD COLUMN IF NOT EXISTS work_start_time time DEFAULT '09:00';
ALTER TABLE tracked_users ADD COLUMN IF NOT EXISTS work_end_time time DEFAULT '17:00';
ALTER TABLE tracked_users ADD COLUMN IF NOT EXISTS timezone text DEFAULT 'America/New_York';
ALTER TABLE tracked_users ADD COLUMN IF NOT EXISTS exclude_weekends boolean DEFAULT true;

-- 2. Create out-of-office table
CREATE TABLE IF NOT EXISTS user_out_of_office (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email text NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    description text,
    created_at timestamptz DEFAULT now(),
    UNIQUE (user_email, start_date, end_date)
);

CREATE INDEX IF NOT EXISTS idx_user_ooo_email ON user_out_of_office (user_email);

-- 3. Add adjusted response hours to response_pairs
ALTER TABLE response_pairs ADD COLUMN IF NOT EXISTS adjusted_response_hours numeric;

-- 4. Add adjusted metrics to daily_stats
ALTER TABLE daily_stats ADD COLUMN IF NOT EXISTS avg_adjusted_hours numeric;
ALTER TABLE daily_stats ADD COLUMN IF NOT EXISTS median_adjusted_hours numeric;
