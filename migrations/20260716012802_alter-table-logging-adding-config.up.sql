-- Add up migration script here
ALTER TABLE "public"."loggings" add column config jsonb;