SELECT 'J_MHB_Vw_Patient_Alert_Tracker_StgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_Staging.vwPatientAlertTracker