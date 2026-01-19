SELECT 'J_CN_PATIENT_BRCA_SCREENING_ASSESSMENT'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
Select
      Hashbite_SSK 
from  edwcr_staging.CN_PATIENT_BRCA_SCREENING_ASSESSMENT_STG 
where Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_PATIENT_BRCA_SCREENING_ASSESSMENT)
)A;