SELECT 'J_MT_REF_NAT_SERV_CENTER'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM  edwcr_staging.Ref_National_Svc_Center_Stg