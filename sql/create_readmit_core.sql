DROP TABLE IF EXISTS public.readmit_core;

CREATE TABLE public.readmit_core AS
SELECT
    a.key_nrd AS key_nrd,
    b.key_nrd AS readmit_key_nrd
FROM
    raw_core a
    INNER JOIN raw_core b ON a.nrd_visitlink = b.nrd_visitlink
WHERE
    b.nrd_daystoevent > a.nrd_daystoevent --Readmit date is after initial admit date
    AND (
        CAST(b.nrd_daystoevent AS INTEGER) - CAST(a.nrd_daystoevent AS INTEGER) - CAST(a.los AS INTEGER)) BETWEEN 1 AND 30 -- Readmit is within 30 days of readmit
    AND a.key_nrd <> b.key_nrd --Not the same visit
    -- AND a.dmonth NOT IN ('1','12')
    AND a.died <> '1' --patient did not die
    AND b.elective <> '1' --not elective readmission
;

