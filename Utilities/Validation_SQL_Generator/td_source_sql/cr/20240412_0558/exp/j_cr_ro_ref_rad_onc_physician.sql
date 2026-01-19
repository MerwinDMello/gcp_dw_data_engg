SELECT 'J_CR_RO_REF_Rad_Onc_Physician'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.stg_DimDoctor