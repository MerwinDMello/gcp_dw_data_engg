export JOBNAME='J_CN_PATIENT_DIAGNOSIS_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_DIAGNOSIS_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from PatientDiagnosis (NOLOCK)"
export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_DIAGNOSIS_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Diagnosis_STG"
