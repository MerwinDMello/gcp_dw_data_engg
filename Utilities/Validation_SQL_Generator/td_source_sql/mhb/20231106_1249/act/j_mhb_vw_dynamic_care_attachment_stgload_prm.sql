SELECT 'J_MHB_Vw_Dynamic_Care_Attachment_stgLoad'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING
FROM EDWCI_STAGING.vwDynamicCareAttachment