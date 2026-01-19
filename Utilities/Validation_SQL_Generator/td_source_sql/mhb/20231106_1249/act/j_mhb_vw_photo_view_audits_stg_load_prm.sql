SELECT 'J_MHB_Vw_Photo_View_Audits_Stg_Load'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_Staging.vwPhotoViewAudits