UPDATE
    nrd_core
SET
    drg = CASE WHEN LENGTH(drg) = 1 THEN
        '00' || drg
    WHEN LENGTH(drg) = 2 THEN
        '0' || drg
    ELSE
        drg
    END;

