SELECT [lngLogID] as log_id
      ,[strUnitID] as unit_num
      ,[strCoID] as coid
      ,[strRequestTypeDesc] as request_type_desc
      ,convert(varchar(19),[datRequestDate], 120) as request_date
      ,case when ltrim(rtrim(substring(strAccount,1,1))) in ('H','D','R') 
            then ltrim(rtrim(substring(strAccount,2,50))) 
        else strAccount end as 
       account_no
      ,[strFinancialClass] as fin_class
      ,convert(varchar(19),[datDateLastActivity], 120) as last_activity_date
      ,[strStatusDesc] as status_desc
      ,convert(varchar(19),[datDiscDate], 120) as disc_date
      ,trim([strDiscSourceDesc]) as disc_source_desc
      ,[strReimbursementImpact] as reimb_impact
      ,[strReprReasons] as repr_reasons
      ,[strQueueName] as queue_name
      ,coalesce([strAccoutPrefix],'') as accountprefix
      ,'v_currtimestamp' as extract_date
  FROM [dbo].[vwClaimsReprocessingTool] with (nolock)
WHERE cast([datDateLastActivity]  as date) >= dateadd(dd,-1,cast(getdate() as date));