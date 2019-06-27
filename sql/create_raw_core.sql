DROP TABLE IF EXISTS public.raw_core;

CREATE TABLE public.raw_core (
    age character varying (20) COLLATE pg_catalog. "default",
    aweekend character varying (20) COLLATE pg_catalog. "default",
    died character varying (20) COLLATE pg_catalog. "default",
    discwt character varying (20) COLLATE pg_catalog. "default",
    dispuniform character varying (20) COLLATE pg_catalog. "default",
    dmonth character varying (20) COLLATE pg_catalog. "default",
    dqtr character varying (20) COLLATE pg_catalog. "default",
    drg character varying (20) COLLATE pg_catalog. "default",
    drgver character varying (20) COLLATE pg_catalog. "default",
    drg_nopoa character varying (20) COLLATE pg_catalog. "default",
    i10_dx1 character varying (20) COLLATE pg_catalog. "default",
    i10_dx2 character varying (20) COLLATE pg_catalog. "default",
    i10_dx3 character varying (20) COLLATE pg_catalog. "default",
    i10_dx4 character varying (20) COLLATE pg_catalog. "default",
    i10_dx5 character varying (20) COLLATE pg_catalog. "default",
    i10_dx6 character varying (20) COLLATE pg_catalog. "default",
    i10_dx7 character varying (20) COLLATE pg_catalog. "default",
    i10_dx8 character varying (20) COLLATE pg_catalog. "default",
    i10_dx9 character varying (20) COLLATE pg_catalog. "default",
    i10_dx10 character varying (20) COLLATE pg_catalog. "default",
    i10_dx11 character varying (20) COLLATE pg_catalog. "default",
    i10_dx12 character varying (20) COLLATE pg_catalog. "default",
    i10_dx13 character varying (20) COLLATE pg_catalog. "default",
    i10_dx14 character varying (20) COLLATE pg_catalog. "default",
    i10_dx15 character varying (20) COLLATE pg_catalog. "default",
    i10_dx16 character varying (20) COLLATE pg_catalog. "default",
    i10_dx17 character varying (20) COLLATE pg_catalog. "default",
    i10_dx18 character varying (20) COLLATE pg_catalog. "default",
    i10_dx19 character varying (20) COLLATE pg_catalog. "default",
    i10_dx20 character varying (20) COLLATE pg_catalog. "default",
    i10_dx21 character varying (20) COLLATE pg_catalog. "default",
    i10_dx22 character varying (20) COLLATE pg_catalog. "default",
    i10_dx23 character varying (20) COLLATE pg_catalog. "default",
    i10_dx24 character varying (20) COLLATE pg_catalog. "default",
    i10_dx25 character varying (20) COLLATE pg_catalog. "default",
    i10_dx26 character varying (20) COLLATE pg_catalog. "default",
    i10_dx27 character varying (20) COLLATE pg_catalog. "default",
    i10_dx28 character varying (20) COLLATE pg_catalog. "default",
    i10_dx29 character varying (20) COLLATE pg_catalog. "default",
    i10_dx30 character varying (20) COLLATE pg_catalog. "default",
    i10_dx31 character varying (20) COLLATE pg_catalog. "default",
    i10_dx32 character varying (20) COLLATE pg_catalog. "default",
    i10_dx33 character varying (20) COLLATE pg_catalog. "default",
    i10_dx34 character varying (20) COLLATE pg_catalog. "default",
    i10_dx35 character varying (20) COLLATE pg_catalog. "default",
    i10_ecause1 character varying (20) COLLATE pg_catalog. "default",
    i10_ecause2 character varying (20) COLLATE pg_catalog. "default",
    i10_ecause3 character varying (20) COLLATE pg_catalog. "default",
    i10_ecause4 character varying (20) COLLATE pg_catalog. "default",
    elective character varying (20) COLLATE pg_catalog. "default",
    female character varying (20) COLLATE pg_catalog. "default",
    hcup_ed character varying (20) COLLATE pg_catalog. "default",
    hosp_nrd character varying (20) COLLATE pg_catalog. "default",
    key_nrd character varying (20) COLLATE pg_catalog. "default",
    los character varying (20) COLLATE pg_catalog. "default",
    mdc character varying (20) COLLATE pg_catalog. "default",
    mdc_nopoa character varying (20) COLLATE pg_catalog. "default",
    i10_ndx character varying (20) COLLATE pg_catalog. "default",
    i10_necause character varying (20) COLLATE pg_catalog. "default",
    i10_npr character varying (20) COLLATE pg_catalog. "default",
    nrd_daystoevent character varying (20) COLLATE pg_catalog. "default",
    nrd_stratum character varying (20) COLLATE pg_catalog. "default",
    nrd_visitlink character varying (20) COLLATE pg_catalog. "default",
    pay1 character varying (20) COLLATE pg_catalog. "default",
    pl_nchs character varying (20) COLLATE pg_catalog. "default",
    i10_pr1 character varying (20) COLLATE pg_catalog. "default",
    i10_pr2 character varying (20) COLLATE pg_catalog. "default",
    i10_pr3 character varying (20) COLLATE pg_catalog. "default",
    i10_pr4 character varying (20) COLLATE pg_catalog. "default",
    i10_pr5 character varying (20) COLLATE pg_catalog. "default",
    i10_pr6 character varying (20) COLLATE pg_catalog. "default",
    i10_pr7 character varying (20) COLLATE pg_catalog. "default",
    i10_pr8 character varying (20) COLLATE pg_catalog. "default",
    i10_pr9 character varying (20) COLLATE pg_catalog. "default",
    i10_pr10 character varying (20) COLLATE pg_catalog. "default",
    i10_pr11 character varying (20) COLLATE pg_catalog. "default",
    i10_pr12 character varying (20) COLLATE pg_catalog. "default",
    i10_pr13 character varying (20) COLLATE pg_catalog. "default",
    i10_pr14 character varying (20) COLLATE pg_catalog. "default",
    i10_pr15 character varying (20) COLLATE pg_catalog. "default",
    prday1 character varying (20) COLLATE pg_catalog. "default",
    prday2 character varying (20) COLLATE pg_catalog. "default",
    prday3 character varying (20) COLLATE pg_catalog. "default",
    prday4 character varying (20) COLLATE pg_catalog. "default",
    prday5 character varying (20) COLLATE pg_catalog. "default",
    prday6 character varying (20) COLLATE pg_catalog. "default",
    prday7 character varying (20) COLLATE pg_catalog. "default",
    prday8 character varying (20) COLLATE pg_catalog. "default",
    prday9 character varying (20) COLLATE pg_catalog. "default",
    prday10 character varying (20) COLLATE pg_catalog. "default",
    prday11 character varying (20) COLLATE pg_catalog. "default",
    prday12 character varying (20) COLLATE pg_catalog. "default",
    prday13 character varying (20) COLLATE pg_catalog. "default",
    prday14 character varying (20) COLLATE pg_catalog. "default",
    prday15 character varying (20) COLLATE pg_catalog. "default",
    rehabtransfer character varying (20) COLLATE pg_catalog. "default",
    resident character varying (20) COLLATE pg_catalog. "default",
    samedayevent character varying (20) COLLATE pg_catalog. "default",
    totchg character varying (20) COLLATE pg_catalog. "default",
    year character varying (20) COLLATE pg_catalog. "default",
    zipinc_qrtl character varying (20) COLLATE pg_catalog. "default",
    dxver character varying (20) COLLATE pg_catalog. "default",
    prver character varying (20) COLLATE pg_catalog. "default")
