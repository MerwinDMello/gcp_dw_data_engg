
SELECT	'J_CN_PATIENT_CONSULTATION'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM
(
SELECT 
Nav_Patient_Lab_Result_SID               
,Hashbite_SSK                  
FROM edwcr_staging.CN_Patient_Lab_Result_stg
where Hashbite_SSK not in ( Select Hashbite_SSK from edwcr.CN_Patient_Lab_Result)
) A;