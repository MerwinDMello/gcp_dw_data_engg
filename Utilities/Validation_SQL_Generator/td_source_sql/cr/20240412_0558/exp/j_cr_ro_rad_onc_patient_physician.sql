SELECT 'J_CR_RO_Rad_Onc_Patient_Physician'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.stg_DIMPATIENTDOCTOR