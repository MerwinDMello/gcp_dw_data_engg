SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from EDWCI.MHB_Group_Chat_Message where dw_last_update_date_time(date)=current_date
 ) Q