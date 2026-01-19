export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_SYSTEM_PRODUCTIVITY_LOG'

export AC_EXP_SQL_STATEMENT="Select 'J_CR_SYSTEM_PRODUCTIVITY_LOG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from EDWCR_STAGING.CR_System_Productivity_Log_WRK"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_SYSTEM_PRODUCTIVITY_LOG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR.CR_System_Productivity_Log"
