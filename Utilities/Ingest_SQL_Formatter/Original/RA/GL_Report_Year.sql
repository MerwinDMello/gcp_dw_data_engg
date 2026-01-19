SELECT
  $SchemaID as Schema_id,
    Me.Ssc_Name,
      Substr(Me.Client_Id, 7,5) as Coid,
      Me.Short_Name as Unit_Num,
      Me.Name as Facility_Name,
     Macp.Cost_Report_Org_Id AS Org_Id,
         Macp.Cost_Report_Year AS Cost_Report_Year,
         MIN (Map.Start_Date) AS Rpt_Yr_Start_Date,
         MAX (Map.End_Date) AS Rpt_Yr_End_Date,
                                current_date as DW_LAST_UPDATE_DATE
    FROM Concuity.Mon_Accounting_Cost_Period Macp
    Inner Join HCA_Reporting.GSU_Me_Org Me
          On Me.Org_Id = Macp.Cost_Report_Org_Id
    Inner Join Concuity.Mon_Accounting_Period Map
          On Macp.Mon_Accounting_Period_Id = Map.Id
                                  GROUP BY Me.Ssc_Name, Me.Client_Id, Me.Short_Name, Me.Name, Macp.Cost_Report_Org_Id, Macp.Cost_Report_Year
  HAVING     MAX (
                CASE
                   WHEN trunc(LAST_DAY(ADD_MONTHS(sysdate, -1))) between Map.Start_Date and Map.End_Date
                   THEN
                      1
                   ELSE
                      0
                END) = 1