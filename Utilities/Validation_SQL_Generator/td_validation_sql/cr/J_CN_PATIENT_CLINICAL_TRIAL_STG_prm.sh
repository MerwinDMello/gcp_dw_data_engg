export JOBNAME='J_CN_PATIENT_CLINICAL_TRIAL_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_CLINICAL_TRIAL_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from PatientClinicalTrial"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_CLINICAL_TRIAL_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Clinical_Trial_Stg;"
