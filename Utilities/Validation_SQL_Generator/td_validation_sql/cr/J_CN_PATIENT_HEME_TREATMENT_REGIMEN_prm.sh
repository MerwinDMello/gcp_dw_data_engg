export JOBNAME='J_CN_PATIENT_HEME_TREATMENT_REGIMEN'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CN_PATIENT_HEME_TREATMENT_REGIMEN'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM  EDWCR_STAGING.Patient_Heme_Treatment_Regimen_STG"

export AC_ACT_SQL_STATEMENT="select 'J_CN_PATIENT_HEME_TREATMENT_REGIMEN'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.CN_Patient_Heme_Treatment_Regimen"


