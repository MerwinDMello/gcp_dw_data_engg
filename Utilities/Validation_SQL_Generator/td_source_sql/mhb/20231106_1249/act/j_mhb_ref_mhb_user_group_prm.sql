SELECT 'J_MHB_Ref_MHB_User_Group'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.Ref_MHB_User_Group where dw_last_update_date_time(date)=current_date
 ) Q