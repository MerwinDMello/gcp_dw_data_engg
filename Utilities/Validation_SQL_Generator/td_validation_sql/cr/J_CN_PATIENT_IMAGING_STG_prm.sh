export JOBNAME='J_CN_PATIENT_IMAGING_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_IMAGING_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from PatientImaging (NOLOCK)"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_IMAGING_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Imaging_STG"
