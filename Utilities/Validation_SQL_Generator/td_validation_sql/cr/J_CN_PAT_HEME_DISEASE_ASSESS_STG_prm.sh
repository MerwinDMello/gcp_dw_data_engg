export JOBNAME='J_CN_PAT_HEME_DISEASE_ASSESS_STG'

export AC_EXP_SQL_STATEMENT="select 'J_CN_PAT_HEME_DISEASE_ASSESS_STG'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[PatientHemeDiseaseAssessment] (NOLOCK)" 

export AC_ACT_SQL_STATEMENT="select 'J_CN_PAT_HEME_DISEASE_ASSESS_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.Patient_Heme_Disease_Assessment_STG"

