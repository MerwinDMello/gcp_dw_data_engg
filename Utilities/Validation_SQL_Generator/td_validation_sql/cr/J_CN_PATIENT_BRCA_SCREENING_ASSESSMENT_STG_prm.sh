export JOBNAME='J_CN_PATIENT_BRCA_SCREENING_ASSESSMENT_STG'



export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_BRCA_SCREENING_ASSESSMENT_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from PatientBRCAScreeningAssessment"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_BRCA_SCREENING_ASSESSMENT_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_patient_BRCA_Screening_Assessment_Stg"

