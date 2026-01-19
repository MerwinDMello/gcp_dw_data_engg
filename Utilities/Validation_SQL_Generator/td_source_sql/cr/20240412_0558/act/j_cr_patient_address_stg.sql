Select 'J_CR_PATIENT_ADDRESS_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR_Staging.CR_PATIENT_ADDRESS_STG