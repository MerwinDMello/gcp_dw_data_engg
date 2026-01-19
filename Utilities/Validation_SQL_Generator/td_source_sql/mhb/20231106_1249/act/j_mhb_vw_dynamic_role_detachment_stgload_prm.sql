SELECT 'J_MHB_Vw_Dynamic_Role_Detachment_stgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_STAGING.vwDynamicRoleDetachment