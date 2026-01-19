export JOBNAME='J_CN_PATIENT_SURV_PLAN_TASK_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_SURV_PLAN_TASK_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from SurvivorShipCarePlan"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_SURV_PLAN_TASK_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Survivorship_Plan_Task_STG;"
