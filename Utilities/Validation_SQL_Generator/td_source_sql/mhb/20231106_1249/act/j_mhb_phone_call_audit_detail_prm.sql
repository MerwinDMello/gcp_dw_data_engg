SELECT 'J_MHB_Phone_Call_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_Phone_Call_Audit_Detail where dw_last_update_date_time(date)=current_date
 ) Q