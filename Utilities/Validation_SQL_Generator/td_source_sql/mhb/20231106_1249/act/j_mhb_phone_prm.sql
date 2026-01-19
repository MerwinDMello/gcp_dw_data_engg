SELECT 'J_MHB_Phone'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_Phone  where dw_last_update_date_time(date)=current_date
 ) Q