CREATE INDEX IF NOT EXISTS along_track_time_idx
    ON along_track USING btree
    (date_time ASC NULLS LAST);