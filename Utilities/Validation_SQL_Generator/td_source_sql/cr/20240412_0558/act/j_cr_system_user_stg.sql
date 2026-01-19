select 'J_CR_System_user_stg'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_Staging.CR_System_User_Stg