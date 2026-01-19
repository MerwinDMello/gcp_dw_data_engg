SELECT 'J_MHB_vwUnits_stgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM edwci_staging.vwUnits