SELECT 'J_CR_RO_REF_RAD_ONC_ADDRESS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.Rad_Onc_Address_wrk