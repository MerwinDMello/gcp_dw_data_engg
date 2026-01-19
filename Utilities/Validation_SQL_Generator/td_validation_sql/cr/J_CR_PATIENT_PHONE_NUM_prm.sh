export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_PHONE_NUM'

export AC_EXP_SQL_STATEMENT="Select 'J_CR_PATIENT_PHONE_NUM_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from EDWCR_Staging.CR_Patient_Phone_Num_Stg"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_PHONE_NUM'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR_BASE_VIEWS.CR_Patient_Phone_Num"
