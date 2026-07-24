ALTER TABLE proofs
ADD COLUMN resolution_source TEXT
    CHECK (
        resolution_source IS NULL
        OR resolution_source IN ('live', 'reconciled')
    );
