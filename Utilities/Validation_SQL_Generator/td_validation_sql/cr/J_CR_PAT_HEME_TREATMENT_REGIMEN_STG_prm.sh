export JOBNAME='J_CR_PAT_HEME_TREATMENT_REGIMEN_STG'

export AC_EXP_SQL_STATEMENT="select 'J_CR_PAT_HEME_TREATMENT_REGIMEN_STG'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[PatientHemeTreatmentRegimen] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_CR_PAT_HEME_TREATMENT_REGIMEN_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.Patient_Heme_Treatment_Regimen_STG"

