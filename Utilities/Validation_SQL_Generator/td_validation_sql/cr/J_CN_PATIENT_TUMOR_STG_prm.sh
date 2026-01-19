export JOBNAME='J_CN_PATIENT_TUMOR_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_TUMOR_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from PatientTumor (NOLOCK)"
export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_TUMOR_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Tumor_STG"
