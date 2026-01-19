SELECT 'J_MHB_Lab_Order_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_LAB_ORDER_AUDIT_DETAIL  where dw_last_update_date_time(date)=current_date
 ) Q