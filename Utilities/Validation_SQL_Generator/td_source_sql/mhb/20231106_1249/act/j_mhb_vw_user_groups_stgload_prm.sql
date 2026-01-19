SELECT 'J_MHB_Vw_User_Groups_StgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_Staging.vwUserGroups