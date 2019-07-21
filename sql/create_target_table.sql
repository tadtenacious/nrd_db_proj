DROP TABLE IF EXISTS public.target_table CASCADE;

CREATE TABLE public.target_table AS SELECT DISTINCT
    a.key_nrd AS key_nrd,
    CASE WHEN b.key_nrd IS NULL THEN
        0
    ELSE
        1
    END target --Target column for machine learning model
FROM
    raw_core a
    LEFT JOIN readmit_core b ON a.key_nrd = b.key_nrd;

