export JOBNAME='J_CN_PATIENT_MLTP_DISP_MEETING_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_MLTP_DISP_MEETING_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from PatientMultiDisciplinaryMeeting"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_MLTP_DISP_MEETING_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Mltp_Disciplinary_Meeting_STG;"
