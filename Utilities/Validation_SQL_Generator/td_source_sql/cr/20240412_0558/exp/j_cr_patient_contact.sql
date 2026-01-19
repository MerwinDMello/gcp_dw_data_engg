Select 'J_CR_PATIENT_CONTACT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from EDWCR_STAGING.CR_PATIENT_CONTACT_WRK