Select 'J_CR_SYSTEM_PRODUCTIVITY_LOG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from EDWCR_STAGING.CR_System_Productivity_Log_WRK