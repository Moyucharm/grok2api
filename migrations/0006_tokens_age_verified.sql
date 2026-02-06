-- Track whether each token already passed imagine age verification.
ALTER TABLE tokens ADD COLUMN age_verified INTEGER NOT NULL DEFAULT 0;
