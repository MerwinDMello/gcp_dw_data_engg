select 'J_CN_PATIENT_HEME_TREATMENT_REGIMEN'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.CN_Patient_Heme_Treatment_Regimen