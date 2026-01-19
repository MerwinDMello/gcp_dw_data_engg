SELECT 'J_MHB_Vw_Inter_Application_Outbound_StgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_Staging.vwInterAppOutbound