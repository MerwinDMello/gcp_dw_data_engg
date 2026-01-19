Select 'J_CR_PATIENT_RADIATION_ONCOLOGY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
edwcr_staging.CR_Patient_Radiation_Oncology_STG