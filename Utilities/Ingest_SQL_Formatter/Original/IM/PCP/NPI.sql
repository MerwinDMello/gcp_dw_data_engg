SELECT 
  DISTINCT CAST(NPINumber AS INT) as NPI_Number, 
  DivisionName AS Division_Name, 
  DivMnemonic AS Division_Mnem, 
  CreatedDate as Create_Date, 
  Provider34ID AS Provider_34_ID,
  'v_currtimestamp' AS dw_last_update_date_time
FROM 
  PCPNotification.dbo.vw_providerenrollment_NPI_34 with (nolock)