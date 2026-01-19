SELECT 'J_MHB_Care_Attachment'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_Care_Attachment where dw_last_update_date_time(date)=current_date
 ) Q