SELECT 'J_MHB_Vw_Inter_Application_Inbound_StgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_Staging.vwInterAppInbound