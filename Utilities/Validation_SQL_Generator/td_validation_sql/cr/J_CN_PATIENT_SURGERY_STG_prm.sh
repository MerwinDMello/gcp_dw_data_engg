export JOBNAME='J_CN_PATIENT_SURGERY_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_SURGERY_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from PatientSurgery"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_SURGERY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_PATIENT_SURGERY_STG;"
