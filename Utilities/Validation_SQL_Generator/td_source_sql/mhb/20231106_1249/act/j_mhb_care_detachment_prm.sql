SELECT 'J_MHB_Care_Detachment'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from EDWCI.MHB_CARE_DETACHMENT where dw_last_update_date_time(date)=current_date
 ) Q