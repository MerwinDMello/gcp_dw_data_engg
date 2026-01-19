SELECT 'J_MHB_User'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_USER  where dw_last_update_date_time(date)=current_date
 ) Q