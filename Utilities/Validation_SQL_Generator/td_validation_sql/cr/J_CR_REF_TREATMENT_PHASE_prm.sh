export JOBNAME='J_CR_REF_TREATMENT_PHASE'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_REF_TREATMENT_PHASE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM  EDWCR_STAGING.Patient_Heme_Treatment_Regimen_STG"

export AC_ACT_SQL_STATEMENT="select 'J_CR_REF_TREATMENT_PHASE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.Ref_Treatment_Phase"


