SELECT 'J_MHB_Vw_User_PhoneCalls_Stg_Load'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM edwci_staging.vwUserPhoneCalls