DECLARE DUP_COUNT INT64;

/*========================================================================
*                                    Hospital Corporation of America
*  This software is proprietary and a trade secret of HCA Information Technology & Services
*  No part of this work may reproduced or used in any form or by any means without written
*  permission of of HCA Information Technology & Services
*
* IM PK Person Activity        Provisioning User Access Controls        Version 1.0        04/18/2018
*
*This macro inserts records into EDWIM.IM_Person_Activity from  EDWIM.EPMS_Provider_Activity
* and various other tables. This macro evalautes the business rule switches to determin if an
*account should be disablemed or protected. If an account is inserted into EDWIM.IM_Person_Activity
*it will be sent for disablement.
*-------------------------------------------------------------------------------------------------------------------------------------
*                                                  R E V I S I O N S
*
*				2022-11-15 - YDF - Add logic to exclude Lawson job class 103.
*-------------------------------------------------------------------------------------------------------------------------------------
*==========================================================================*/ BEGIN
DELETE
FROM edwim.im_person_activity
WHERE rtrim(im_person_activity.source_system_code) IN('R',
                                                      'E',
                                                      'D');

BEGIN TRANSACTION;


INSERT INTO edwim.im_person_activity (im_domain_id, im_person_user_id, esaf_activity_date, access_rule_id, im_person_inactivate_sw, source_system_code, dw_last_update_date_time)
SELECT ep3.im_domain_id,
       ep3.im_person_user_id,
       ep3.esaf_activity_date,
       ep3.access_rule_id,
       ep3.im_person_inactivate_sw,
       ep3.source_system_code,
       ep3.dw_last_update_date_time
FROM
  (SELECT DISTINCT ep2.im_domain_id,
                   ep2.epms_user_id AS im_person_user_id,
                   ep2.esaf_activity_date,
                   ep2.access_rule_id,
                   CASE
                       WHEN ep2.access_rule_id = 0 THEN ep2.access_rule_id
                       ELSE 1
                   END AS im_person_inactivate_sw,
                   ep2.source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT -- NON-CONTROLLED--
 ep.im_domain_id,
 ep.epms_user_id,
 ep.esaf_activity_date, -- BEGIN RULES--
 CASE
     WHEN trim(coalesce(imd.im_domain_name, '')) IN('25901',
                                                    '00322') THEN 0
     WHEN trim(coalesce(imd.im_domain_name, '')) = '27720' THEN 0
     WHEN ep.mt_active_txf_provider_sw = 1 THEN 0
     WHEN ep.cactus_privileged_provider_sw = 1 THEN 0
     WHEN ep.mt_active_provider_sw = 1 THEN 0
     WHEN ljc.lawson_excluded_job_class_sw = 1 THEN 0
     ELSE CASE
              WHEN ep.cactus_privileged_provider_sw = 0 THEN 1
              WHEN coalesce(ep.mt_active_txf_provider_sw, 0) = 0
                   OR coalesce(ep.mt_active_provider_sw, 0) = 0 THEN 2
              ELSE 0
          END
 END AS access_rule_id, -- Divested Facilities (OUMC)
 -- Facilities not in MEDITECH
 -- 'ACTIVE PROVIDER RECORDS (SAN ANTONIO/TXF MARKET ONLY) 3.3
 -- ACTIVE CACTUS FACTILITY STATUS 3.1
 -- ACTIVE Resident / Fellow PROVIDER RECORDS IN THE MIS PROVIDER DICTIONARY 3.2
 -- Exclude active employees in Lawson Job Class 103
 -- INACTIVE CACTUS FACTILITY STATUS 3.1
 -- 'INACTIVE PROVIDER RECORDS (SAN ANTONIO/TXF MARKET ONLY) 3.3
 --  'INACTIVE or Non - Resident / Fellow PROVIDER RECORDS IN THE MIS PROVIDER DICTIONARY 3.2
 ep.source_system_code 
      FROM edwim_base_views.epms_provider_activity AS ep 
      INNER JOIN edwim_base_views.ref_im_domain AS imd ON ep.im_domain_id = imd.im_domain_id 
      AND ep.source_system_code = imd.source_system_code 
      LEFT OUTER JOIN 
        (SELECT hre.employee_34_login_code, 
                1 AS lawson_excluded_job_class_sw 
         FROM 
           (SELECT jbcode.job_code_sid, 
                   jclcode.job_class_code 
            FROM edwim_base_views.hr_job_code AS jbcode 
            INNER JOIN edwim_base_views.hr_job_class AS jclcode ON jbcode.job_class_sid = jclcode.job_class_sid 
            AND CAST(/* expression of unknown or erroneous type */ jclcode.valid_to_date AS DATE) = DATE '9999-12-31' 
            AND upper(rtrim(jclcode.source_system_code)) = 'L' 
            AND upper(rtrim(jclcode.active_dw_ind)) = 'Y' 
            WHERE rtrim(jclcode.job_class_code) = '103' 
              AND CAST(/* expression of unknown or erroneous type */ jbcode.valid_to_date AS DATE) = DATE '9999-12-31' 
              AND upper(rtrim(jbcode.source_system_code)) = 'L' 
              AND upper(rtrim(jbcode.active_dw_ind)) = 'Y' ) AS jcd 
         INNER JOIN 
           (SELECT hr_job_position.position_sid, 
                   hr_job_position.lawson_company_num, 
                   hr_job_position.process_level_code, 
                   hr_job_position.job_code_sid 
            FROM edwim_base_views.hr_job_position 
            WHERE CAST(/* expression of unknown or erroneous type */ hr_job_position.eff_to_date AS DATE) = DATE '9999-12-31' 
              AND CAST(/* expression of unknown or erroneous type */ hr_job_position.valid_to_date AS DATE) = DATE '9999-12-31' 
              AND upper(rtrim(hr_job_position.source_system_code)) = 'L' 
              AND upper(rtrim(hr_job_position.active_dw_ind)) = 'Y' ) AS jpos ON jcd.job_code_sid = jpos.job_code_sid 
         INNER JOIN 
           (SELECT hr_employee_position.employee_sid, 
                   hr_employee_position.lawson_company_num, 
                   hr_employee_position.process_level_code, 
                   hr_employee_position.position_sid 
            FROM edwim_base_views.hr_employee_position 
            WHERE CAST(/* expression of unknown or erroneous type */ hr_employee_position.valid_to_date AS DATE) = DATE '9999-12-31' 
              AND CAST(/* expression of unknown or erroneous type */ hr_employee_position.eff_to_date AS DATE) = DATE '9999-12-31' 
              AND upper(rtrim(hr_employee_position.source_system_code)) = 'L' 
              AND upper(rtrim(hr_employee_position.active_dw_ind)) = 'Y' ) AS pos ON pos.position_sid = jpos.position_sid 
         AND pos.lawson_company_num = jpos.lawson_company_num 
         AND pos.process_level_code = jpos.process_level_code 
         INNER JOIN 
           (SELECT hr_employee.employee_34_login_code, 
                   hr_employee.employee_sid, 
                   hr_employee.lawson_company_num, 
                   hr_employee.process_level_code 
            FROM edwim_base_views.hr_employee 
            WHERE CAST(/* expression of unknown or erroneous type */ hr_employee.valid_to_date AS DATE) = DATE '9999-12-31' 
              AND CAST(/* expression of unknown or erroneous type */ hr_employee.termination_date AS DATE) = DATE '1800-01-01' 
              AND upper(rtrim(hr_employee.active_dw_ind)) = 'Y' 
              AND upper(rtrim(hr_employee.source_system_code)) = 'L' ) AS hre ON hre.employee_sid = pos.employee_sid 
         AND hre.lawson_company_num = pos.lawson_company_num 
         AND hre.process_level_code = pos.process_level_code 
         GROUP BY 1) AS ljc ON ep.epms_user_id = ljc.employee_34_login_code 
      WHERE upper(rtrim(ep.source_system_code)) = 'R' 
      UNION DISTINCT SELECT -- CONTROLLED--
 ep.im_domain_id, 
 ep.epms_user_id, 
 ep.esaf_activity_date, -- BEGIN RULES--
 CASE 
     WHEN trim(coalesce(imd.im_domain_name, '')) IN('25901', 
                                                    '00322') THEN 0 
     WHEN trim(coalesce(imd.im_domain_name, '')) = '27720' THEN 0 
     WHEN ep.mt_active_provider_sw = 1 THEN 0 
     WHEN ep.mt_active_txf_provider_sw = 1 
          AND coalesce(CAST(bqutil.fn.cw_td_normalize_number(mds.mdstaff_active_dea_license_sw) AS FLOAT64), CAST(bqutil.fn.cw_td_normalize_number(format('%4d', 0)) AS FLOAT64)) = 1 THEN 0 
     WHEN ep.cactus_privileged_provider_sw = 1 
          AND ep.cactus_active_dea_license_sw = 1 THEN 0 
     WHEN ljc.lawson_excluded_job_class_sw = 1 THEN 0 
     ELSE CASE 
              WHEN ep.cactus_privileged_provider_sw = 1 
                   AND ep.cactus_active_dea_license_sw = 0 THEN 3 
              WHEN ep.cactus_privileged_provider_sw = 0 THEN 1 
              WHEN ep.cactus_privileged_provider_sw IS NULL THEN CASE 
                                                                     WHEN ep.mt_active_txf_provider_sw = 1 
                                                                          AND coalesce(CAST(bqutil.fn.cw_td_normalize_number(mds.mdstaff_active_dea_license_sw) AS FLOAT64), CAST(bqutil.fn.cw_td_normalize_number(format('%4d', 0)) AS FLOAT64)) = 0 THEN 3 
                                                                     WHEN ep.mt_active_provider_sw = 0 
                                                                          OR ep.mt_active_txf_provider_sw = 0 THEN 2 
                                                                     ELSE 0 
                                                                 END 
          END 
 END AS access_rule_id, -- Divested Facilities (OUMC)
 -- Facilities not in MEDITECH
 -- ACTIVE Resident / Fellow PROVIDER RECORDS IN THE MIS PROVIDER DICTIONARY 3.2
 -- 'ACTIVE PROVIDER RECORDS (SAN ANTONIO/TXF MARKET ONLY) 5.3
 -- WITH ACTIVE DEA LICENSE
 -- ACTIVE CACTUS FACTILITY STATUS 5.1
 -- ACTIVE DEA LICENSE 5.4
 -- Exclude active employees in Lawson Job Class 103
 -- ACTIVE CACTUS FACTILITY STATUS 5.1
 -- INACTIVE DEA LICENSE IN CACTUS
 -- INACTIVE CACTUS FACTILITY STATUS 5.1
 -- NOT FOUND IN CACTUS 5.1
 -- 'ACTIVE PROVIDER RECORDS (SAN ANTONIO/TXF MARKET ONLY) 5.3
 -- INACTIVE DEA LICENSE IN MDStaff
 --  INACTIVE or Non - Resident / Fellow PROVIDER RECORDS IN THE MIS PROVIDER DICTIONARY 3.2
 -- 'INACTIVE PROVIDER RECORDS (SAN ANTONIO/TXF MARKET ONLY) 5.3
 ep.source_system_code
      FROM edwim_base_views.epms_provider_activity AS ep
      INNER JOIN edwim_base_views.ref_im_domain AS imd ON ep.im_domain_id = imd.im_domain_id
      AND ep.source_system_code = imd.source_system_code
      LEFT OUTER JOIN
        (SELECT fac.im_domain_id,
                max(trim(coalesce(pd.provider_npi, ''))) AS mdstaff_provider_npi,
                '1' AS mdstaff_active_dea_license_sw
         FROM edwim_base_views.provider_license AS pl
         INNER JOIN edwim_base_views.provider_detail AS pd ON pd.provider_id = pl.provider_id
         AND pd.source_system_code = pl.source_system_code
         LEFT OUTER JOIN edwim_base_views.ref_provider_license_status AS rls ON rls.licn_stts_id = pl.licn_status_id
         AND rls.source_system_code = pl.source_system_code
         LEFT OUTER JOIN edwim_base_views.ref_provider_license_type AS rplt ON rplt.licn_type_id = pl.licn_type_id
         AND rplt.source_system_code = pl.source_system_code
         INNER JOIN
           (SELECT imd_0.im_domain_id,
                   imd_0.im_domain_name,
                   max(trim(coalesce(fac_0.facility_state_code, ''))) AS facility_state_code
            FROM edwim_base_views.ref_im_domain AS imd_0
            INNER JOIN edwim_views.company_master AS fac_0 ON upper(trim(coalesce(imd_0.im_domain_name, ''))) = upper(trim(coalesce(fac_0.unit_num, '')))
            WHERE upper(rtrim(imd_0.source_system_code)) = 'E'
              AND trim(coalesce(fac_0.facility_state_code, '')) <> ''
            GROUP BY 1,
                     2,
                     upper(trim(coalesce(fac_0.facility_state_code, '')))) AS fac ON upper(rtrim(fac.facility_state_code)) = upper(trim(coalesce(pl.licn_state_name, '')))
         AND trim(coalesce(pl.licn_state_name, '')) <> ''
         WHERE upper(rtrim(pl.source_system_code)) = 'S'
           AND upper(upper(trim(coalesce(rplt.licn_type_desc, '')))) LIKE '%DEA%'
           AND trim(coalesce(pd.provider_npi, '')) <> ''
           AND CAST(/* expression of unknown or erroneous type */ pl.licn_expire_date AS DATE) >= date_sub(current_date(), interval 3 DAY)
         GROUP BY 1,
                  upper(trim(coalesce(pd.provider_npi, '')))) AS mds ON mds.im_domain_id = ep.im_domain_id
      AND upper(trim(coalesce(ep.epms_prov_npi, ''))) = upper(rtrim(mds.mdstaff_provider_npi))
      AND trim(coalesce(ep.epms_prov_npi, '')) <> ''
      LEFT OUTER JOIN
        (SELECT hre.employee_34_login_code,
                1 AS lawson_excluded_job_class_sw
         FROM
           (SELECT jbcode.job_code_sid,
                   jclcode.job_class_code
            FROM edwim_base_views.hr_job_code AS jbcode
            INNER JOIN edwim_base_views.hr_job_class AS jclcode ON jbcode.job_class_sid = jclcode.job_class_sid
            AND CAST(/* expression of unknown or erroneous type */ jclcode.valid_to_date AS DATE) = DATE '9999-12-31'
            AND upper(rtrim(jclcode.source_system_code)) = 'L'
            AND upper(rtrim(jclcode.active_dw_ind)) = 'Y'
            WHERE rtrim(jclcode.job_class_code) = '103'
              AND CAST(/* expression of unknown or erroneous type */ jbcode.valid_to_date AS DATE) = DATE '9999-12-31'
              AND upper(rtrim(jbcode.source_system_code)) = 'L'
              AND upper(rtrim(jbcode.active_dw_ind)) = 'Y' ) AS jcd
         INNER JOIN
           (SELECT hr_job_position.position_sid,
                   hr_job_position.lawson_company_num,
                   hr_job_position.process_level_code,
                   hr_job_position.job_code_sid
            FROM edwim_base_views.hr_job_position
            WHERE CAST(/* expression of unknown or erroneous type */ hr_job_position.eff_to_date AS DATE) = DATE '9999-12-31'
              AND CAST(/* expression of unknown or erroneous type */ hr_job_position.valid_to_date AS DATE) = DATE '9999-12-31'
              AND upper(rtrim(hr_job_position.source_system_code)) = 'L'
              AND upper(rtrim(hr_job_position.active_dw_ind)) = 'Y' ) AS jpos ON jcd.job_code_sid = jpos.job_code_sid
         INNER JOIN
           (SELECT hr_employee_position.employee_sid,
                   hr_employee_position.lawson_company_num,
                   hr_employee_position.process_level_code,
                   hr_employee_position.position_sid
            FROM edwim_base_views.hr_employee_position
            WHERE CAST(/* expression of unknown or erroneous type */ hr_employee_position.valid_to_date AS DATE) = DATE '9999-12-31'
              AND CAST(/* expression of unknown or erroneous type */ hr_employee_position.eff_to_date AS DATE) = DATE '9999-12-31'
              AND upper(rtrim(hr_employee_position.source_system_code)) = 'L'
              AND upper(rtrim(hr_employee_position.active_dw_ind)) = 'Y' ) AS pos ON pos.position_sid = jpos.position_sid
         AND pos.lawson_company_num = jpos.lawson_company_num
         AND pos.process_level_code = jpos.process_level_code
         INNER JOIN
           (SELECT hr_employee.employee_34_login_code,
                   hr_employee.employee_sid,
                   hr_employee.lawson_company_num,
                   hr_employee.process_level_code
            FROM edwim_base_views.hr_employee
            WHERE CAST(/* expression of unknown or erroneous type */ hr_employee.valid_to_date AS DATE) = DATE '9999-12-31'
              AND CAST(/* expression of unknown or erroneous type */ hr_employee.termination_date AS DATE) = DATE '1800-01-01'
              AND upper(rtrim(hr_employee.active_dw_ind)) = 'Y'
              AND upper(rtrim(hr_employee.source_system_code)) = 'L' ) AS hre ON hre.employee_sid = pos.employee_sid
         AND hre.lawson_company_num = pos.lawson_company_num
         AND hre.process_level_code = pos.process_level_code
         GROUP BY 1) AS ljc ON ep.epms_user_id = ljc.employee_34_login_code
      WHERE upper(rtrim(ep.source_system_code)) = 'E' ) AS ep2) AS ep3
WHERE ep3.im_person_inactivate_sw = 1;


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT im_domain_id,
             im_person_user_id,
             esaf_activity_date
      FROM `hca-hin-dev-cur-comp`.edwim.im_person_activity
      GROUP BY im_domain_id,
               im_person_user_id,
               esaf_activity_date
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-comp`.edwim.im_person_activity');

ELSE
COMMIT TRANSACTION;

END IF;

END;