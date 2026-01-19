Select 'J_CR_PATIENT_MEDICAL_ONCOLOGY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
EDWCR_STAGING.CR_Patient_Medical_Oncology_STG