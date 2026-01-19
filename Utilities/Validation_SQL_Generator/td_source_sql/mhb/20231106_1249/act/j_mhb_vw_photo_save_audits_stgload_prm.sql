SELECT 'J_MHB_Vw_Photo_Save_Audits_StgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_Staging.vwPhotoSaveAudits