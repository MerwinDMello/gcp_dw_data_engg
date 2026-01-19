export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_ADDRESS_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CR_PATIENT_ADDRESS_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from mregistry.dbo.Patient"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_ADDRESS_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR_Staging.CR_PATIENT_ADDRESS_STG"
