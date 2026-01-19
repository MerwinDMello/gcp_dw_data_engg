export JOBNAME='J_CN_PATIENT_HEME_TRANSPLANT_STG'

export AC_EXP_SQL_STATEMENT="select 'J_CN_PATIENT_HEME_TRANSPLANT_STG'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[PatientHemeTransplant] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_HEME_TRANSPLANT_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.stg_PatientHemeTransplant"

