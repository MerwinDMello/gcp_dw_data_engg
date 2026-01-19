export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_TUMOR'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_PATIENT_TUMOR'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM  $NCR_STG_SCHEMA.CR_Patient_Tumor_STG"

export AC_ACT_SQL_STATEMENT="select 'J_CR_PATIENT_TUMOR'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CR_Patient_Tumor"


