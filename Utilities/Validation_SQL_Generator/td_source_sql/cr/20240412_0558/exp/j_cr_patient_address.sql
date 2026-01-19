Select 'J_CR_PATIENT_ADDRESS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from EDWCR_STAGING.CR_PATIENT_ADDRESS_WRK