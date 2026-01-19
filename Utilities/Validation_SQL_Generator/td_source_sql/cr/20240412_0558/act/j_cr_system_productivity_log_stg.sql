Select 'J_CR_SYSTEM_PRODUCTIVITY_LOG_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
edwcr_staging.CR_System_Productivity_Log_Stg