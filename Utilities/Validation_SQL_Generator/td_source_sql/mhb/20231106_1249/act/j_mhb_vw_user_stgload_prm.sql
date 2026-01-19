SELECT 'J_MHB_Vw_User_StgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM edwci_staging.vw_user_stg