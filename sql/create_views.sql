DROP Materialized View IF EXISTS temp_prior_frequency CASCADE;

DROP Materialized View IF EXISTS temp_prior_recency CASCADE;

-------------------------------------------------------

CREATE Materialized View temp_prior_frequency AS
-- CREATE LOCAL TEMP TABLE temp_prior_frequency AS
SELECT 
		 ncs.nrd_visitlink  --designates the unique pt
		,ncs.key_nrd  --designates the unique index visit, use to join to main data set
		,count(pv_ncs.key_nrd) pif_visits  --count of visits in the prior thirty days to an index event
		,sum(case when (pv_ncs.aweekend::INT = 0 or pv_ncs.aweekend::INT = 1) then pv_ncs.aweekend::INT else 0 end) pif_aweekend --Admission day is on a weekend
		,sum(case when (pv_ncs.elective::INT = 0 or pv_ncs.elective::INT = 1) then pv_ncs.elective::INT else 0 end) pif_elective  --Elective versus non-elective admission
 		,sum(case when pv_ncs.hcup_ed::INT = 0 then 0 else 1 end) pif_hcup_ed  --HCUP indicator of emergency department record
 		,count(distinct pv_ncs.hosp_nrd) pif_hosp_nrd  --	HCUP NRD hospital identification number
 		,sum(case when pv_ncs.los::INT >= 0 and pv_ncs.los::INT <= 365 then pv_ncs.los::INT else 0 end) pif_los --Length of stay, cleaned
 		,sum(pv_ncs.i10_ndx::INT) pif_i10_ndx --Number of ICD-10-CM diagnoses on this discharge
 		,sum(pv_ncs.i10_necause::INT) pif_i10_necause --Number of ICD-10-CM External Cause of Morbidity codes on this record
 		,sum(pv_ncs.i10_npr::INT) pif_i10_npr --Number of ICD-10-PCS procedures on this discharge
-- msf medical (M) or surgical procedure (P)
 		,sum(case when msf.msf = 'M' then 1 else 0 end) pif_msf_M   --medical procedure
		,sum(case when msf.msf = 'P' then 1 else 0 end) pif_msf_P   --surgical procedure
--  ,dispuniform
 		,sum(case when pv_ncs.dispuniform::INT = 1 then 1 else 0 end) pif_dispuniform_routine  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  1	Routine
		,sum(case when pv_ncs.dispuniform::INT = 2 then 1 else 0 end) pif_dispuniform_sth  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  2	Transfer to Short-term Hospital
		,sum(case when pv_ncs.dispuniform::INT = 5 then 1 else 0 end) pif_dispuniform_otherMedFac  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  5	Transfer Other Including Skilled Nursing Facility (SNF), Intermediate Care Facility (ICF), Another Type
		,sum(case when pv_ncs.dispuniform::INT = 6 then 1 else 0 end) pif_dispuniform_hhc  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  6	Home Health Care (HHC)
		,sum(case when pv_ncs.dispuniform::INT = 7 then 1 else 0 end) pif_dispuniform_ama  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  7	Against Medical Advice (AMA)
		,sum(case when pv_ncs.dispuniform::INT = 99 then 1 else 0 end) pif_dispuniform_unk  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  99	Discharge alive, destination unknown
--  ,pv_ncs.pay1		
 		,sum(case when pv_ncs.pay1::INT = 1 then 1 else 0 end) pif_pay1_1 --Expected primary payer, uniform 	Medicare
 		,sum(case when pv_ncs.pay1::INT = 2 then 1 else 0 end) pif_pay1_2 --Expected primary payer, uniform 	Medicaid
 		,sum(case when pv_ncs.pay1::INT = 3 then 1 else 0 end) pif_pay1_3 --Expected primary payer, uniform   Private insurance
 		,sum(case when pv_ncs.pay1::INT = 4 then 1 else 0 end) pif_pay1_4 --Expected primary payer, uniform   Self-pay
 		,sum(case when pv_ncs.pay1::INT = 5 then 1 else 0 end) pif_pay1_5 --Expected primary payer, uniform   No charge
 		,sum(case when pv_ncs.pay1::INT = 6 then 1 else 0 end) pif_pay1_6 --Expected primary payer, uniform   Other
 		,sum(case when pv_ncs.pay1::INT NOT IN(1,2,3,4,5,6) then 1 else 0 end) pif_pay1_7 --Expected primary payer, uniform  **Missing
--  ,pv_ncs.pl_nchs	
		,sum(case when pv_ncs.pl_nchs::INT = 1 then 1 else 0 end) pif_pl_nchs_1 --Patient Location: NCHS Urban-Rural Code   "Central" counties of metro areas of >=1 million population
 		,sum(case when pv_ncs.pl_nchs::INT = 2 then 1 else 0 end) pif_pl_nchs_2 --Patient Location: NCHS Urban-Rural Code   "Fringe" counties of metro areas of >=1 million population
 		,sum(case when pv_ncs.pl_nchs::INT = 3 then 1 else 0 end) pif_pl_nchs_3 --Patient Location: NCHS Urban-Rural Code   Counties in metro areas of 250,000-999,999 population
 		,sum(case when pv_ncs.pl_nchs::INT = 4 then 1 else 0 end) pif_pl_nchs_4 --Patient Location: NCHS Urban-Rural Code   Counties in metro areas of 50,000-249,999 population
 		,sum(case when pv_ncs.pl_nchs::INT = 5 then 1 else 0 end) pif_pl_nchs_5 --Patient Location: NCHS Urban-Rural Code   Micropolitan counties
 		,sum(case when pv_ncs.pl_nchs::INT = 6 then 1 else 0 end) pif_pl_nchs_6 --Patient Location: NCHS Urban-Rural Code   Not metropolitan or micropolitan counties
 		,sum(case when pv_ncs.pl_nchs::INT NOT IN(1,2,3,4,5,6) then 1 else 0 end) pif_pl_nchs_7   --Patient Location: NCHS Urban-Rural Code  **Missing
-- 		,pv_ncs.prday1
	  ,sum(case when pv_ncs.prday1::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday1_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday1::INT IN(0) then 1 else 0 end) pif_prday1_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday1::INT >0 and pv_ncs.prday1::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday1_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday2
	  ,sum(case when pv_ncs.prday2::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday2_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday2::INT IN(0) then 1 else 0 end) pif_prday2_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday2::INT >0 and pv_ncs.prday2::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday2_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday3
	  ,sum(case when pv_ncs.prday3::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday3_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday3::INT IN(0) then 1 else 0 end) pif_prday3_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday3::INT >0 and pv_ncs.prday3::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday3_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday4
	  ,sum(case when pv_ncs.prday4::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday4_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday4::INT IN(0) then 1 else 0 end) pif_prday4_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday4::INT >0 and pv_ncs.prday4::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday4_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday5
	  ,sum(case when pv_ncs.prday5::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday5_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday5::INT IN(0) then 1 else 0 end) pif_prday5_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday5::INT >0 and pv_ncs.prday5::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday5_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday6
	  ,sum(case when pv_ncs.prday6::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday6_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday6::INT IN(0) then 1 else 0 end) pif_prday6_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday6::INT >0 and pv_ncs.prday6::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday6_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday7
	  ,sum(case when pv_ncs.prday7::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday7_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday7::INT IN(0) then 1 else 0 end) pif_prday7_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday7::INT >0 and pv_ncs.prday7::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday7_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday8
	  ,sum(case when pv_ncs.prday8::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday8_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday8::INT IN(0) then 1 else 0 end) pif_prday8_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday8::INT >0 and pv_ncs.prday8::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday8_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday9
	  ,sum(case when pv_ncs.prday9::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday9_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday9::INT IN(0) then 1 else 0 end) pif_prday9_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday9::INT >0 and pv_ncs.prday9::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday9_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday10
	  ,sum(case when pv_ncs.prday10::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday10_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday10::INT IN(0) then 1 else 0 end) pif_prday10_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday10::INT >0 and pv_ncs.prday10::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday10_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday11
	  ,sum(case when pv_ncs.prday11::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday11_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday11::INT IN(0) then 1 else 0 end) pif_prday11_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday11::INT >0 and pv_ncs.prday11::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday11_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday12
	  ,sum(case when pv_ncs.prday12::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday12_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday12::INT IN(0) then 1 else 0 end) pif_prday12_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday12::INT >0 and pv_ncs.prday12::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday12_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday13
	  ,sum(case when pv_ncs.prday13::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday13_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday13::INT IN(0) then 1 else 0 end) pif_prday13_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday13::INT >0 and pv_ncs.prday13::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday13_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday14
	  ,sum(case when pv_ncs.prday14::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday14_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday14::INT IN(0) then 1 else 0 end) pif_prday14_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday14::INT >0 and pv_ncs.prday14::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday14_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday15
	  ,sum(case when pv_ncs.prday15::INT IN(-4,-3,-2,-1) then 1 else 0 end) pif_prday15_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday15::INT IN(0) then 1 else 0 end) pif_prday15_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday15::INT >0 and pv_ncs.prday15::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pif_prday15_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
 		,sum(pv_ncs.rehabtransfer::INT) pif_rehabtransfer   --A combined record involving transfer to rehabilitation, evaluation, or other aftercare
 		,sum(pv_ncs.resident::INT) pif_resident   --Identifies patient as a resident of the State in which he or she received hospital care
 		,sum(case when pv_ncs.samedayevent::INT = 0 then 1 else 0 end) pif_samedayevent_noxfer  --	Identifies transfer and same-day stay collapsed records, 	Not a transfer or other same-day stay
 		,sum(case when pv_ncs.samedayevent::INT = 1 then 1 else 0 end) pif_samedayevent_xfer  --	Identifies transfer and same-day stay collapsed records, 	Transfer involving two discharges from different hospitals
 		,sum(case when pv_ncs.samedayevent::INT = 2 then 1 else 0 end) pif_samedayevent_diffHosp  --	Identifies transfer and same-day stay collapsed records, 		Same-day stay involving two discharges from different hospitals
 		,sum(case when pv_ncs.samedayevent::INT = 3 then 1 else 0 end) pif_samedayevent_sameHosp  --	Identifies transfer and same-day stay collapsed records, 	Same-day stay involving two discharges at the same hospital
 		,sum(case when pv_ncs.samedayevent::INT = 4 then 1 else 0 end) pif_samedayevent_3orMoredisch  --	Identifies transfer and same-day stay collapsed records, 	Same-day stay involving three or more discharges at the same or different hospitals		
 		,sum(case when pv_ncs.totchg::INT NOT IN(-999999999) then pv_ncs.totchg::INT else 0 end) pif_totalchg   --Total charges, cleaned

FROM 
	nrd_core AS ncs 
	LEFT JOIN nrd_core AS pv_ncs 
			ON ncs.nrd_visitlink = pv_ncs.nrd_visitlink
	LEFT JOIN lu_drg_msf AS msf
			ON pv_ncs.drgver = msf.drgver AND
				 pv_ncs.drg = msf.drg
				 
WHERE 
	1=1
	AND ncs.dmonth NOT IN ('1','12')
	AND ncs.dispuniform <> '20'
	AND ncs.key_nrd <> pv_ncs.key_nrd
	AND ncs.nrd_daystoevent >= pv_ncs.nrd_daystoevent
	AND (ncs.nrd_daystoevent::INT - pv_ncs.los::INT - pv_ncs.nrd_daystoevent::INT) <= 30
	
/*For testing*/	
	--AND ncs.nrd_visitlink = 'd4p2zwd'
	--AND ncs.key_nrd IN('201614652835')
	
GROUP BY 
		 ncs.nrd_visitlink
		,ncs.key_nrd;
		
-------------------------------------------------------

CREATE Materialized View temp_prior_recency AS
-- CREATE LOCAL TEMP TABLE temp_prior_recency AS

select tr.* from  
(SELECT 
		 ncs.nrd_visitlink  --designates the unique pt
		,ncs.key_nrd  --designates the unique index visit, use to join to main data set
		,ncs.nrd_daystoevent pir_ncs_day_disch  --reference value used to help verify
		,pv_ncs.nrd_daystoevent pir_pv_day
		,ncs.drg pir_curr_drg
		,pv_ncs.drg pir_prev_drg
		,case when ncs.drg = pv_ncs.drg then 1 else 0 end pir_drg_match
		,ROW_NUMBER () OVER (PARTITION BY ncs.nrd_visitlink, ncs.nrd_daystoevent ORDER BY pv_ncs.nrd_daystoevent desc) pir_mostrecent_admit
		,(ncs.nrd_daystoevent::INT - pv_ncs.los::INT - pv_ncs.nrd_daystoevent::INT) pir_days_between
		,ncs.target pir_target
		,count(pv_ncs.key_nrd) pir_visits  --count of visits in the prior thirty days to an index event
		,sum(case when (pv_ncs.aweekend::INT = 0 or pv_ncs.aweekend::INT = 1) then pv_ncs.aweekend::INT else 0 end) pir_aweekend --Admission day is on a weekend
		,sum(case when (pv_ncs.elective::INT = 0 or pv_ncs.elective::INT = 1) then pv_ncs.elective::INT else 0 end) pir_elective  --Elective versus non-elective admission
 		,sum(case when pv_ncs.hcup_ed::INT = 0 then 0 else 1 end) pir_hcup_ed  --HCUP indicator of emergency department record
 		,count(distinct pv_ncs.hosp_nrd) pir_hosp_nrd  --	HCUP NRD hospital identification number
 		,sum(case when pv_ncs.los::INT >= 0 and pv_ncs.los::INT <= 365 then pv_ncs.los::INT else 0 end) pir_los --Length of stay, cleaned
 		,sum(pv_ncs.i10_ndx::INT) pir_i10_ndx --Number of ICD-10-CM diagnoses on this discharge
 		,sum(pv_ncs.i10_necause::INT) pir_i10_necause --Number of ICD-10-CM External Cause of Morbidity codes on this record
 		,sum(pv_ncs.i10_npr::INT) pir_i10_npr --Number of ICD-10-PCS procedures on this discharge
-- msf medical (M) or surgical procedure (P)
 		,sum(case when msf.msf = 'M' then 1 else 0 end) pir_msf_M   --medical procedure
		,sum(case when msf.msf = 'P' then 1 else 0 end) pir_msf_P   --surgical procedure
--  ,dispuniform
 		,sum(case when pv_ncs.dispuniform::INT = 1 then 1 else 0 end) pir_dispuniform_routine  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  1	Routine
		,sum(case when pv_ncs.dispuniform::INT = 2 then 1 else 0 end) pir_dispuniform_sth  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  2	Transfer to Short-term Hospital
		,sum(case when pv_ncs.dispuniform::INT = 5 then 1 else 0 end) pir_dispuniform_otherMedFac  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  5	Transfer Other Including Skilled Nursing Facility (SNF), Intermediate Care Facility (ICF), Another Type
		,sum(case when pv_ncs.dispuniform::INT = 6 then 1 else 0 end) pir_dispuniform_hhc  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  6	Home Health Care (HHC)
		,sum(case when pv_ncs.dispuniform::INT = 7 then 1 else 0 end) pir_dispuniform_ama  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  7	Against Medical Advice (AMA)
		,sum(case when pv_ncs.dispuniform::INT = 99 then 1 else 0 end) pir_dispuniform_unk  --disposition of the patient at discharge (routine, transfer to another hospital, died, etc.  99	Discharge alive, destination unknown
--  ,pv_ncs.pay1		
 		,sum(case when pv_ncs.pay1::INT = 1 then 1 else 0 end) pir_pay1_1 --Expected primary payer, uniform 	Medicare
 		,sum(case when pv_ncs.pay1::INT = 2 then 1 else 0 end) pir_pay1_2 --Expected primary payer, uniform 	Medicaid
 		,sum(case when pv_ncs.pay1::INT = 3 then 1 else 0 end) pir_pay1_3 --Expected primary payer, uniform   Private insurance
 		,sum(case when pv_ncs.pay1::INT = 4 then 1 else 0 end) pir_pay1_4 --Expected primary payer, uniform   Self-pay
 		,sum(case when pv_ncs.pay1::INT = 5 then 1 else 0 end) pir_pay1_5 --Expected primary payer, uniform   No charge
 		,sum(case when pv_ncs.pay1::INT = 6 then 1 else 0 end) pir_pay1_6 --Expected primary payer, uniform   Other
 		,sum(case when pv_ncs.pay1::INT NOT IN(1,2,3,4,5,6) then 1 else 0 end) pir_pay1_7 --Expected primary payer, uniform  **Missing
--  ,pv_ncs.pl_nchs	
		,sum(case when pv_ncs.pl_nchs::INT = 1 then 1 else 0 end) pir_pl_nchs_1 --Patient Location: NCHS Urban-Rural Code   "Central" counties of metro areas of >=1 million population
 		,sum(case when pv_ncs.pl_nchs::INT = 2 then 1 else 0 end) pir_pl_nchs_2 --Patient Location: NCHS Urban-Rural Code   "Fringe" counties of metro areas of >=1 million population
 		,sum(case when pv_ncs.pl_nchs::INT = 3 then 1 else 0 end) pir_pl_nchs_3 --Patient Location: NCHS Urban-Rural Code   Counties in metro areas of 250,000-999,999 population
 		,sum(case when pv_ncs.pl_nchs::INT = 4 then 1 else 0 end) pir_pl_nchs_4 --Patient Location: NCHS Urban-Rural Code   Counties in metro areas of 50,000-249,999 population
 		,sum(case when pv_ncs.pl_nchs::INT = 5 then 1 else 0 end) pir_pl_nchs_5 --Patient Location: NCHS Urban-Rural Code   Micropolitan counties
 		,sum(case when pv_ncs.pl_nchs::INT = 6 then 1 else 0 end) pir_pl_nchs_6 --Patient Location: NCHS Urban-Rural Code   Not metropolitan or micropolitan counties
 		,sum(case when pv_ncs.pl_nchs::INT NOT IN(1,2,3,4,5,6) then 1 else 0 end) pir_pl_nchs_7   --Patient Location: NCHS Urban-Rural Code  **Missing
-- 		,pv_ncs.prday1
	  ,sum(case when pv_ncs.prday1::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday1_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday1::INT IN(0) then 1 else 0 end) pir_prday1_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday1::INT >0 and pv_ncs.prday1::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday1_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday2
	  ,sum(case when pv_ncs.prday2::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday2_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday2::INT IN(0) then 1 else 0 end) pir_prday2_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday2::INT >0 and pv_ncs.prday2::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday2_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday3
	  ,sum(case when pv_ncs.prday3::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday3_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday3::INT IN(0) then 1 else 0 end) pir_prday3_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday3::INT >0 and pv_ncs.prday3::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday3_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday4
	  ,sum(case when pv_ncs.prday4::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday4_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday4::INT IN(0) then 1 else 0 end) pir_prday4_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday4::INT >0 and pv_ncs.prday4::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday4_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday5
	  ,sum(case when pv_ncs.prday5::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday5_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday5::INT IN(0) then 1 else 0 end) pir_prday5_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday5::INT >0 and pv_ncs.prday5::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday5_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday6
	  ,sum(case when pv_ncs.prday6::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday6_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday6::INT IN(0) then 1 else 0 end) pir_prday6_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday6::INT >0 and pv_ncs.prday6::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday6_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday7
	  ,sum(case when pv_ncs.prday7::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday7_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday7::INT IN(0) then 1 else 0 end) pir_prday7_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday7::INT >0 and pv_ncs.prday7::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday7_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday8
	  ,sum(case when pv_ncs.prday8::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday8_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday8::INT IN(0) then 1 else 0 end) pir_prday8_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday8::INT >0 and pv_ncs.prday8::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday8_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday9
	  ,sum(case when pv_ncs.prday9::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday9_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday9::INT IN(0) then 1 else 0 end) pir_prday9_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday9::INT >0 and pv_ncs.prday9::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday9_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday10
	  ,sum(case when pv_ncs.prday10::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday10_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday10::INT IN(0) then 1 else 0 end) pir_prday10_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday10::INT >0 and pv_ncs.prday10::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday10_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday11
	  ,sum(case when pv_ncs.prday11::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday11_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday11::INT IN(0) then 1 else 0 end) pir_prday11_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday11::INT >0 and pv_ncs.prday11::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday11_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday12
	  ,sum(case when pv_ncs.prday12::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday12_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday12::INT IN(0) then 1 else 0 end) pir_prday12_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday12::INT >0 and pv_ncs.prday12::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday12_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday13
	  ,sum(case when pv_ncs.prday13::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday13_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday13::INT IN(0) then 1 else 0 end) pir_prday13_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday13::INT >0 and pv_ncs.prday13::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday13_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday14
	  ,sum(case when pv_ncs.prday14::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday14_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday14::INT IN(0) then 1 else 0 end) pir_prday14_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday14::INT >0 and pv_ncs.prday14::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday14_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
-- 		,pv_ncs.prday15
	  ,sum(case when pv_ncs.prday15::INT IN(-4,-3,-2,-1) then 1 else 0 end) pir_prday15_preadmit  --Number of days from admission to procedure n, -4 - -1	Days prior to admission
		,sum(case when pv_ncs.prday15::INT IN(0) then 1 else 0 end) pir_prday15_admit  --Number of days from admission to procedure n, 0	Day of admission
		,sum(case when pv_ncs.prday15::INT >0 and pv_ncs.prday15::INT <= (pv_ncs.los::INT +3) then 1 else 0 end) pir_prday15_postadmit  --Number of days from admission to procedure n, 1 - LOS+3	Days after admission
 		,sum(pv_ncs.rehabtransfer::INT) pir_rehabtransfer   --A combined record involving transfer to rehabilitation, evaluation, or other aftercare
 		,sum(pv_ncs.resident::INT) pir_resident   --Identifies patient as a resident of the State in which he or she received hospital care
 		,sum(case when pv_ncs.samedayevent::INT = 0 then 1 else 0 end) pir_samedayevent_noxfer  --	Identifies transfer and same-day stay collapsed records, 	Not a transfer or other same-day stay
 		,sum(case when pv_ncs.samedayevent::INT = 1 then 1 else 0 end) pir_samedayevent_xfer  --	Identifies transfer and same-day stay collapsed records, 	Transfer involving two discharges from different hospitals
 		,sum(case when pv_ncs.samedayevent::INT = 2 then 1 else 0 end) pir_samedayevent_diffHosp  --	Identifies transfer and same-day stay collapsed records, 		Same-day stay involving two discharges from different hospitals
 		,sum(case when pv_ncs.samedayevent::INT = 3 then 1 else 0 end) pir_samedayevent_sameHosp  --	Identifies transfer and same-day stay collapsed records, 	Same-day stay involving two discharges at the same hospital
 		,sum(case when pv_ncs.samedayevent::INT = 4 then 1 else 0 end) pir_samedayevent_3orMoredisch  --	Identifies transfer and same-day stay collapsed records, 	Same-day stay involving three or more discharges at the same or different hospitals		
 		,sum(case when pv_ncs.totchg::INT NOT IN(-999999999) then pv_ncs.totchg::INT else 0 end) pir_totalchg   --Total charges, cleaned

FROM 
	nrd_core AS ncs 
	LEFT JOIN nrd_core AS pv_ncs 
			ON ncs.nrd_visitlink = pv_ncs.nrd_visitlink
	LEFT JOIN lu_drg_msf AS msf
			ON  pv_ncs.drgver = msf.drgver
			AND	pv_ncs.drg = msf.drg
				 
WHERE 
	1=1
	AND ncs.dmonth NOT IN ('1','12')
	AND ncs.dispuniform <> '20'
	AND ncs.key_nrd <> pv_ncs.key_nrd
	AND ncs.nrd_daystoevent::INT >= pv_ncs.nrd_daystoevent::INT
	AND (ncs.nrd_daystoevent::INT - pv_ncs.los::INT - pv_ncs.nrd_daystoevent::INT) <= 30
	AND (ncs.nrd_daystoevent::INT - pv_ncs.los::INT - pv_ncs.nrd_daystoevent::INT) >=0

	
/*For testing*/	
	--AND ncs.nrd_visitlink = 'd52cgvj'
	--AND ncs.key_nrd IN('201614652835')
	
GROUP BY 
		 ncs.nrd_visitlink  --designates the unique pt
		,ncs.key_nrd  --designates the unique index visit, use to join to main data set
		,ncs.nrd_daystoevent  --reference value used to help verify
		,ncs.los 
		,pv_ncs.nrd_daystoevent 
		,ncs.drg
		,pv_ncs.drg
		,case when ncs.drg = pv_ncs.drg then 1 else 0 end
		,(ncs.nrd_daystoevent::INT - pv_ncs.los::INT - pv_ncs.nrd_daystoevent::INT) 
		,ncs.target 

ORDER BY 
		 ncs.nrd_visitlink  --designates the unique pt
		,ncs.nrd_daystoevent::INT 
		,pv_ncs.nrd_daystoevent::INT) as tr

WHERE 
		pir_mostrecent_admit = 1;
		
---------------------------------------
DROP MATERIALIZED VIEW  IF EXISTS feature_set;
CREATE MATERIALIZED VIEW feature_set AS
SELECT DISTINCT
c.target,
c.age,
c.aweekend,
-- c.died,
-- c.discwt,
c.dispuniform,
-- c.dmonth,
c.dqtr,
c.drg,
-- c.drgver,
-- c.drg_nopoa,
c.i10_dx1,
c.i10_dx2,
c.i10_dx3,
-- c.i10_dx4,
-- c.i10_dx5,
-- c.i10_dx6,
-- c.i10_dx7,
-- c.i10_dx8,
-- c.i10_dx9,
-- c.i10_dx10,
-- c.i10_dx11,
-- c.i10_dx12,
-- c.i10_dx13,
-- c.i10_dx14,
-- c.i10_dx15,
-- c.i10_dx16,
-- c.i10_dx17,
-- c.i10_dx18,
-- c.i10_dx19,
-- c.i10_dx20,
-- c.i10_dx21,
-- c.i10_dx22,
-- c.i10_dx23,
-- c.i10_dx24,
-- c.i10_dx25,
-- c.i10_dx26,
-- c.i10_dx27,
-- c.i10_dx28,
-- c.i10_dx29,
-- c.i10_dx30,
-- c.i10_dx31,
-- c.i10_dx32,
-- c.i10_dx33,
-- c.i10_dx34,
-- c.i10_dx35,
-- c.i10_ecause1,
-- c.i10_ecause2,
-- c.i10_ecause3,
-- c.i10_ecause4,
-- (Case when c.i10_ecause1 = '' Then 0 else 1 END) +
-- (Case when c.i10_ecause2 = '' Then 0 else 1 END) +
-- (Case when c.i10_ecause3 = '' Then 0 else 1 END) +
-- (Case when c.i10_ecause4 = '' Then 0 else 1 END) i10_ecause_count,
c.elective,
c.female,
c.hcup_ed,
-- c.hosp_nrd,
-- c.key_nrd,
c.los,
c.mdc,
-- c.mdc_nopoa,
c.i10_ndx,
c.i10_necause,
c.i10_npr,
-- c.nrd_daystoevent,
c.nrd_stratum,
c.nrd_visitlink,
c.pay1,
c.pl_nchs,
c.i10_pr1,
c.i10_pr2,
c.i10_pr3,
-- c.i10_pr4,
-- c.i10_pr5,
-- c.i10_pr6,
-- c.i10_pr7,
-- c.i10_pr8,
-- c.i10_pr9,
-- c.i10_pr10,
-- c.i10_pr11,
-- c.i10_pr12,
-- c.i10_pr13,
-- c.i10_pr14,
-- c.i10_pr15,
c.prday1,
c.prday2,
c.prday3,
c.prday4,
c.prday5,
c.prday6,
c.prday7,
c.prday8,
c.prday9,
c.prday10,
c.prday11,
c.prday12,
c.prday13,
c.prday14,
c.prday15,
c.rehabtransfer,
c.resident,
c.samedayevent,
c.totchg,
-- c.year,
c.zipinc_qrtl,
-- c.dxver,
-- c.prver,
h.hosp_bedsize,
h.h_contrl,
h.hosp_urcat4,
h.hosp_ur_teach,
h.n_disc_u,
h.n_hosp_u,
h.s_disc_u,
h.s_hosp_u,
h.total_disc,
s.aprdrg_risk_mortality,
s.aprdrg_severity,
case when lu_msf.msf = 'M' then 1 else 0 end medSurgFlag_M,
lu_drg."drgName",
lu_mdc.mdc_name,
lu_mdc.mdc_short,
case when lu_mdc.mdc_short in('0') then 1 else 0 end mdc_NotAssign,
case when lu_mdc.mdc_short in('1') then 1 else 0 end mdc_Nervous,
case when lu_mdc.mdc_short in('2') then 1 else 0 end mdc_Eye,
case when lu_mdc.mdc_short in('3') then 1 else 0 end mdc_ENTM,
case when lu_mdc.mdc_short in('4') then 1 else 0 end mdc_Respiratory,
case when lu_mdc.mdc_short in('5') then 1 else 0 end mdc_Circulatory,
case when lu_mdc.mdc_short in('6') then 1 else 0 end mdc_Digestive,
case when lu_mdc.mdc_short in('7') then 1 else 0 end mdc_Hepatobiliary,
case when lu_mdc.mdc_short in('8') then 1 else 0 end mdc_MusculoSkeletal,
case when lu_mdc.mdc_short in('9') then 1 else 0 end mdc_Skin,
case when lu_mdc.mdc_short in('10') then 1 else 0 end mdc_Endocrine,
case when lu_mdc.mdc_short in('11') then 1 else 0 end mdc_KidneyUrinary,
case when lu_mdc.mdc_short in('12') then 1 else 0 end mdc_MaleReprod,
case when lu_mdc.mdc_short in('13') then 1 else 0 end mdc_FemaleReprod,
case when lu_mdc.mdc_short in('14') then 1 else 0 end mdc_Pregnancy,
case when lu_mdc.mdc_short in('15') then 1 else 0 end mdc_Newborns,
case when lu_mdc.mdc_short in('16') then 1 else 0 end mdc_Blood,
case when lu_mdc.mdc_short in('17') then 1 else 0 end mdc_Myeloproliferative,
case when lu_mdc.mdc_short in('18') then 1 else 0 end mdc_InfectParasitic,
case when lu_mdc.mdc_short in('19') then 1 else 0 end mdc_Mental,
case when lu_mdc.mdc_short in('20') then 1 else 0 end mdc_AlcoholDrug,
case when lu_mdc.mdc_short in('21') then 1 else 0 end mdc_Poisoning,
case when lu_mdc.mdc_short in('22') then 1 else 0 end mdc_Burns,
case when lu_mdc.mdc_short in('23') then 1 else 0 end mdc_HlthSvcs,
case when lu_mdc.mdc_short in('24') then 1 else 0 end mdc_MultiTrauma,
case when lu_mdc.mdc_short in('25') then 1 else 0 end mdc_HIV,
case when lu_mdc.mdc_short in('.') then 1 else 0 end mdc_Missing,
---------- recency
COALESCE(r.pir_drg_match,0) pir_drg_match,
COALESCE(r.pir_days_between,0) pir_days_between,
COALESCE(r.pir_aweekend,0) pir_aweekend,
COALESCE(r.pir_elective,0) pir_elective,
COALESCE(r.pir_hcup_ed,0) pir_hcup_ed,
COALESCE(r.pir_hosp_nrd,0) pir_hosp_nrd,
COALESCE(r.pir_los,0) pir_los,
COALESCE(r.pir_i10_ndx,0) pir_i10_ndx,
COALESCE(r.pir_i10_necause,0) pir_i10_necause,
COALESCE(r.pir_i10_npr,0) pir_i10_npr,
COALESCE(r.pir_msf_M,0) pir_msf_M,
COALESCE(r.pir_msf_P,0) pir_msf_P,
COALESCE(r.pir_pay1_1,0) pir_pay1_1,
COALESCE(r.pir_pay1_2,0) pir_pay1_2,
COALESCE(r.pir_pay1_3,0) pir_pay1_3,
COALESCE(r.pir_pay1_4,0) pir_pay1_4,
COALESCE(r.pir_pay1_5,0) pir_pay1_5,
COALESCE(r.pir_pay1_6,0) pir_pay1_6,
COALESCE(r.pir_pay1_7,0) pir_pay1_7,
COALESCE(r.pir_pl_nchs_1,0) pir_pl_nchs_1,
COALESCE(r.pir_pl_nchs_2,0) pir_pl_nchs_2,
COALESCE(r.pir_pl_nchs_3,0) pir_pl_nchs_3,
COALESCE(r.pir_pl_nchs_4,0) pir_pl_nchs_4,
COALESCE(r.pir_pl_nchs_5,0) pir_pl_nchs_5,
COALESCE(r.pir_pl_nchs_6,0) pir_pl_nchs_6,
COALESCE(r.pir_pl_nchs_7,0) pir_pl_nchs_7,
COALESCE(r.pir_prday1_preadmit,0) pir_prday1_preadmit,
COALESCE(r.pir_prday1_admit,0) pir_prday1_admit,
COALESCE(r.pir_prday1_postadmit,0) pir_prday1_postadmit,
COALESCE(r.pir_prday2_preadmit,0) pir_prday2_preadmit,
COALESCE(r.pir_prday2_admit,0) pir_prday2_admit,
COALESCE(r.pir_prday2_postadmit,0) pir_prday2_postadmit,
COALESCE(r.pir_prday3_preadmit,0) pir_prday3_preadmit,
COALESCE(r.pir_prday3_admit,0) pir_prday3_admit,
COALESCE(r.pir_prday3_postadmit,0) pir_prday3_postadmit,
COALESCE(r.pir_prday4_preadmit,0) pir_prday4_preadmit,
COALESCE(r.pir_prday4_admit,0) pir_prday4_admit,
COALESCE(r.pir_prday4_postadmit,0) pir_prday4_postadmit,
COALESCE(r.pir_prday5_preadmit,0) pir_prday5_preadmit,
COALESCE(r.pir_prday5_admit,0) pir_prday5_admit,
COALESCE(r.pir_prday5_postadmit,0) pir_prday5_postadmit,
COALESCE(r.pir_prday6_preadmit,0) pir_prday6_preadmit,
COALESCE(r.pir_prday6_admit,0) pir_prday6_admit,
COALESCE(r.pir_prday6_postadmit,0) pir_prday6_postadmit,
COALESCE(r.pir_prday7_preadmit,0) pir_prday7_preadmit,
COALESCE(r.pir_prday7_admit,0) pir_prday7_admit,
COALESCE(r.pir_prday7_postadmit,0) pir_prday7_postadmit,
COALESCE(r.pir_prday8_preadmit,0) pir_prday8_preadmit,
COALESCE(r.pir_prday8_admit,0) pir_prday8_admit,
COALESCE(r.pir_prday8_postadmit,0) pir_prday8_postadmit,
COALESCE(r.pir_prday9_preadmit,0) pir_prday9_preadmit,
COALESCE(r.pir_prday9_admit,0) pir_prday9_admit,
COALESCE(r.pir_prday9_postadmit,0) pir_prday9_postadmit,
COALESCE(r.pir_prday10_preadmit,0) pir_prday10_preadmit,
COALESCE(r.pir_prday10_admit,0) pir_prday10_admit,
COALESCE(r.pir_prday10_postadmit,0) pir_prday10_postadmit,
COALESCE(r.pir_prday11_preadmit,0) pir_prday11_preadmit,
COALESCE(r.pir_prday11_admit,0) pir_prday11_admit,
COALESCE(r.pir_prday11_postadmit,0) pir_prday11_postadmit,
COALESCE(r.pir_prday12_preadmit,0) pir_prday12_preadmit,
COALESCE(r.pir_prday12_admit,0) pir_prday12_admit,
COALESCE(r.pir_prday12_postadmit,0) pir_prday12_postadmit,
COALESCE(r.pir_prday13_preadmit,0) pir_prday13_preadmit,
COALESCE(r.pir_prday13_admit,0) pir_prday13_admit,
COALESCE(r.pir_prday13_postadmit,0) pir_prday13_postadmit,
COALESCE(r.pir_prday14_preadmit,0) pir_prday14_preadmit,
COALESCE(r.pir_prday14_admit,0) pir_prday14_admit,
COALESCE(r.pir_prday14_postadmit,0) pir_prday14_postadmit,
COALESCE(r.pir_prday15_preadmit,0) pir_prday15_preadmit,
COALESCE(r.pir_prday15_admit,0) pir_prday15_admit,
COALESCE(r.pir_prday15_postadmit,0) pir_prday15_postadmit,
COALESCE(r.pir_rehabtransfer,0) pir_rehabtransfer,
COALESCE(r.pir_resident,0) pir_resident,
COALESCE(r.pir_samedayevent_noxfer,0) pir_samedayevent_noxfer,
COALESCE(r.pir_samedayevent_xfer,0) pir_samedayevent_xfer,
COALESCE(r.pir_samedayevent_diffhosp,0) pir_samedayevent_diffhosp,
COALESCE(r.pir_samedayevent_samehosp,0) pir_samedayevent_samehosp,
COALESCE(r.pir_samedayevent_3ormoredisch,0) pir_samedayevent_3ormoredisch,
COALESCE(r.pir_totalchg,0) pir_totalchg,
---------- frequency		
COALESCE(f.pif_visits,0) pif_visits,
COALESCE(f.pif_aweekend,0) pif_aweekend,
COALESCE(f.pif_elective,0) pif_elective,
COALESCE(f.pif_hcup_ed,0) pif_hcup_ed,
COALESCE(f.pif_hosp_nrd,0) pif_hosp_nrd,
COALESCE(f.pif_los,0) pif_los,
COALESCE(f.pif_i10_ndx,0) pif_i10_ndx,
COALESCE(f.pif_i10_necause,0) pif_i10_necause,
COALESCE(f.pif_i10_npr,0) pif_i10_npr,
COALESCE(f.pif_msf_M,0) pif_msf_M,
COALESCE(f.pif_msf_P,0) pif_msf_P,
COALESCE(f.pif_pay1_1,0) pif_pay1_1,
COALESCE(f.pif_pay1_2,0) pif_pay1_2,
COALESCE(f.pif_pay1_3,0) pif_pay1_3,
COALESCE(f.pif_pay1_4,0) pif_pay1_4,
COALESCE(f.pif_pay1_5,0) pif_pay1_5,
COALESCE(f.pif_pay1_6,0) pif_pay1_6,
COALESCE(f.pif_pay1_7,0) pif_pay1_7,
COALESCE(f.pif_pl_nchs_1,0) pif_pl_nchs_1,
COALESCE(f.pif_pl_nchs_2,0) pif_pl_nchs_2,
COALESCE(f.pif_pl_nchs_3,0) pif_pl_nchs_3,
COALESCE(f.pif_pl_nchs_4,0) pif_pl_nchs_4,
COALESCE(f.pif_pl_nchs_5,0) pif_pl_nchs_5,
COALESCE(f.pif_pl_nchs_6,0) pif_pl_nchs_6,
COALESCE(f.pif_pl_nchs_7,0) pif_pl_nchs_7,
COALESCE(f.pif_prday1_preadmit,0) pif_prday1_preadmit,
COALESCE(f.pif_prday1_admit,0) pif_prday1_admit,
COALESCE(f.pif_prday1_postadmit,0) pif_prday1_postadmit,
COALESCE(f.pif_prday2_preadmit,0) pif_prday2_preadmit,
COALESCE(f.pif_prday2_admit,0) pif_prday2_admit,
COALESCE(f.pif_prday2_postadmit,0) pif_prday2_postadmit,
COALESCE(f.pif_prday3_preadmit,0) pif_prday3_preadmit,
COALESCE(f.pif_prday3_admit,0) pif_prday3_admit,
COALESCE(f.pif_prday3_postadmit,0) pif_prday3_postadmit,
COALESCE(f.pif_prday4_preadmit,0) pif_prday4_preadmit,
COALESCE(f.pif_prday4_admit,0) pif_prday4_admit,
COALESCE(f.pif_prday4_postadmit,0) pif_prday4_postadmit,
COALESCE(f.pif_prday5_preadmit,0) pif_prday5_preadmit,
COALESCE(f.pif_prday5_admit,0) pif_prday5_admit,
COALESCE(f.pif_prday5_postadmit,0) pif_prday5_postadmit,
COALESCE(f.pif_prday6_preadmit,0) pif_prday6_preadmit,
COALESCE(f.pif_prday6_admit,0) pif_prday6_admit,
COALESCE(f.pif_prday6_postadmit,0) pif_prday6_postadmit,
COALESCE(f.pif_prday7_preadmit,0) pif_prday7_preadmit,
COALESCE(f.pif_prday7_admit,0) pif_prday7_admit,
COALESCE(f.pif_prday7_postadmit,0) pif_prday7_postadmit,
COALESCE(f.pif_prday8_preadmit,0) pif_prday8_preadmit,
COALESCE(f.pif_prday8_admit,0) pif_prday8_admit,
COALESCE(f.pif_prday8_postadmit,0) pif_prday8_postadmit,
COALESCE(f.pif_prday9_preadmit,0) pif_prday9_preadmit,
COALESCE(f.pif_prday9_admit,0) pif_prday9_admit,
COALESCE(f.pif_prday9_postadmit,0) pif_prday9_postadmit,
COALESCE(f.pif_prday10_preadmit,0) pif_prday10_preadmit,
COALESCE(f.pif_prday10_admit,0) pif_prday10_admit,
COALESCE(f.pif_prday10_postadmit,0) pif_prday10_postadmit,
COALESCE(f.pif_prday11_preadmit,0) pif_prday11_preadmit,
COALESCE(f.pif_prday11_admit,0) pif_prday11_admit,
COALESCE(f.pif_prday11_postadmit,0) pif_prday11_postadmit,
COALESCE(f.pif_prday12_preadmit,0) pif_prday12_preadmit,
COALESCE(f.pif_prday12_admit,0) pif_prday12_admit,
COALESCE(f.pif_prday12_postadmit,0) pif_prday12_postadmit,
COALESCE(f.pif_prday13_preadmit,0) pif_prday13_preadmit,
COALESCE(f.pif_prday13_admit,0) pif_prday13_admit,
COALESCE(f.pif_prday13_postadmit,0) pif_prday13_postadmit,
COALESCE(f.pif_prday14_preadmit,0) pif_prday14_preadmit,
COALESCE(f.pif_prday14_admit,0) pif_prday14_admit,
COALESCE(f.pif_prday14_postadmit,0) pif_prday14_postadmit,
COALESCE(f.pif_prday15_preadmit,0) pif_prday15_preadmit,
COALESCE(f.pif_prday15_admit,0) pif_prday15_admit,
COALESCE(f.pif_prday15_postadmit,0) pif_prday15_postadmit,
COALESCE(f.pif_rehabtransfer,0) pif_rehabtransfer,
COALESCE(f.pif_resident,0) pif_resident,
COALESCE(f.pif_samedayevent_noxfer,0) pif_samedayevent_noxfer,
COALESCE(f.pif_samedayevent_xfer,0) pif_samedayevent_xfer,
COALESCE(f.pif_samedayevent_diffhosp,0) pif_samedayevent_diffhosp,
COALESCE(f.pif_samedayevent_samehosp,0) pif_samedayevent_samehosp,
COALESCE(f.pif_samedayevent_3ormoredisch,0) pif_samedayevent_3ormoredisch,
COALESCE(f.pif_totalchg,0) pif_totalchg

FROM 
	nrd_core c
	LEFT JOIN raw_hospital h 
		ON c.hosp_nrd::BIGINT = h.hosp_nrd
	LEFT JOIN raw_severity s 
		ON c.key_nrd::BIGINT = s.key_nrd
	LEFT JOIN temp_prior_recency r 
		ON c.key_nrd = r.key_nrd
	LEFT JOIN temp_prior_frequency f 
		ON c.key_nrd = f.key_nrd

LEFT JOIN lu_drg_msf AS lu_msf
		ON  c.drgver = lu_msf.drgver
		AND	c.drg = lu_msf.drg

LEFT JOIN lu_mdc_names as lu_mdc
		ON  c.mdc = lu_mdc.mdc
		AND	c.drgver = lu_mdc.drgver
	LEFT JOIN lu_drg_names as lu_drg 
		ON  c.drg = lu_drg.drg
		AND	c.drgver = lu_drg.drgver

WHERE 
	1=1
	AND c.dmonth NOT IN ('1','12')
	AND c.died = '0';




DROP Materialized View IF EXISTS visitlink_sample CASCADE;
CREATE MATERIALIZED VIEW visitlink_sample
AS
 SELECT DISTINCT a.nrd_visitlink
   FROM ( SELECT raw_core.nrd_visitlink
           FROM raw_core
         LIMIT 150000) a
 LIMIT 10000;


DROP Materialized View IF EXISTS feature_set_sample CASCADE;
CREATE Materialized View feature_set_sample
AS
SELECT a.* FROM feature_set a
INNER JOIN visitlink_sample b ON a.nrd_visitlink = b.nrd_visitlink;

-- CREATE Materialized View feature_set_sample AS

-- SELECT * FROM feature_set TABLESAMPLE system (1) REPEATABLE (101);