SELECT 'J_Remittance_PLB_Adj' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
SELECT
A.Payment_GUID,A.Audit_Date AS Audit_Date
,A.Adj_Code AS Adj_Reason_Code
, A.Adj_Id AS Adj_Ref_Id
, row_number()  over ( partition by payment_GUID,adj_code,Audit_date,Adj_Id order by payment_GUID asc )AS Adj_Record_Num
,A.Delete_Ind AS Delete_Ind
--,coalesce(delete_date,cast('1999-01-01' as date))  AS Delete_Date
,Delete_Date
,A.Adj_Amt
,A.Adj_Match AS Adj_Match_Code
,A.Claim_Control_Num AS EP_Calc_Claim_Control_Num
,A.Fiscal_Period_Date
 ,'E' AS   Source_System_Code
,Current_Timestamp(0) AS DW_Last_Update_Date_Time
FROM edwpbs_staging.Remittance_PLB_Adj A
)a
