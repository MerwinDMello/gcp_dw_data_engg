SELECT 'J_MHB_Broadcast_Message_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_Broadcast_Message_Detail  where dw_last_update_date_time(date)=current_date
 ) Q