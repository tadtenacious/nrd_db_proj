DROP TABLE IF EXISTS public.nrd_core;

CREATE TABLE public.nrd_core AS
SELECT
    a.*,
    t.target
FROM
    raw_core a
    INNER JOIN target_table t ON a.key_nrd = t.key_nrd
