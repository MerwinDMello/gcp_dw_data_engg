SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from EDWCI.Ref_MHB_WCTP_Source_Name  where dw_last_update_date_time(date)=current_date
 ) Q