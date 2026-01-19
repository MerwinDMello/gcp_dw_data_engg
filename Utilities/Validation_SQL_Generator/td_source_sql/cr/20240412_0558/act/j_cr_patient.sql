Select 'J_CR_PATIENT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR.CR_PATIENT