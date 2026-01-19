select 
  t1.providerid as md_staff_provider_id, 
  t1.facilityid as md_staff_primary_facility_id, 
  t1.formname as md_staff_form_name, 
  t1.privilegetype as md_staff_privilege_type_name, 
  t1.privilegedesc as md_staff_privilege_desc, 
  t1.privilegestatus as md_staff_status_code, 
  t1.privilegestatusdesc as md_staff_status_desc, 
  t1.casescompleted as md_staff_cases_completed, 
  t1.granteddate as md_staff_granted_date, 
  t1.expireddate as md_staff_expired_date, 
  t1.commenttext1 as md_staff_comment_text_1, 
  t1.commenttext2 as md_staff_comment_text_2, 
  t2.downloaddate as md_staff_download_date, 
  'S' AS source_system_code, 
  'v_currtimestamp' AS dw_last_update_date_time 
FROM 
  (
    SELECT 
      DISTINCT CAST(
        [providerid] AS VARCHAR(50)
      ) AS [providerid], 
      CAST(
        [FacilityID] AS varchar(255)
      ) AS facilityid, 
      CAST(
        [FormName] AS VARCHAR(255)
      ) AS formname, 
      CAST(
        [Type] AS VARCHAR(255)
      ) AS privilegetype, 
      CAST(
        [Privilege] AS VARCHAR(255)
      ) AS privilegedesc, 
      CAST(
        [Status] AS VARCHAR(255)
      ) AS privilegestatus, 
      CAST(
        [StatusDescription] AS VARCHAR(255)
      ) AS privilegestatusdesc, 
      casescompleted, 
      CAST([Granted] AS DATE) AS granteddate, 
      CAST([Expired] AS DATE) AS expireddate, 
      CAST(
        [Comment] AS VARCHAR(255)
      ) AS commenttext1, 
      CAST(
        [Comments] AS VARCHAR(255)
      ) AS commenttext2 
    FROM 
      [MD-Staff_Import].MDStaff.PrivilegeData WITH (NOLOCK) 
    WHERE 
      providerid IS NOT NULL
  ) t1 
  INNER JOIN (
    SELECT 
      DISTINCT CAST(
        providerid AS VARCHAR(50)
      ) AS providerid, 
      CAST(DownloadDate AS DATETIME) AS downloaddate 
    FROM 
      [MDStaff].dbo.vw_ProviderData WITH (NOLOCK)
  ) t2 on t1.providerid = t2.providerid
