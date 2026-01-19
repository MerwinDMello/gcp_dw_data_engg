Select 'J_CR_PATIENT_PHONE_NUM_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR_Staging.CR_Patient_Phone_Num_Stg