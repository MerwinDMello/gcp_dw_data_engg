Select 'J_CR_PATIENT_PHONE_NUM'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR_BASE_VIEWS.CR_Patient_Phone_Num