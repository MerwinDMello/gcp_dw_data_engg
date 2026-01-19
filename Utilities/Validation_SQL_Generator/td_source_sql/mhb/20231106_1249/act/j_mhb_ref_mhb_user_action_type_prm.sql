SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from EDWCI.Ref_MHB_User_Action_Type  where dw_last_update_date_time(date)=current_date
 ) Q