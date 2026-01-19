DECLARE DUP_COUNT INT64;

/*========================================================================
*                                    Hospital Corporation of America
*  This software is proprietary and a trade secret of HCA Information Technology & Services
*  No part of this work may reproduced or used in any form or by any means without written
*  permission of of HCA Information Technology & Services
*
* EPMS_Provider_Status      Provisioning User Access Controls        Version 1.0        04/04/2018
*-------------------------------------------------------------------------------------------------------------------------------------
*                                                  R E V I S I O N S

*-------------------------------------------------------------------------------------------------------------------------------------
*
*   2020/12/07 - YDF - Modified EPMS_PROV_ACTIVE_FLAG logic for EPCS to utilize new vendor file column indicating facility level provider status.
*
*==========================================================================*/ BEGIN TRUNCATE TABLE edwim.epms_provider_status;

BEGIN TRANSACTION;


INSERT INTO edwim.epms_provider_status (im_domain_id, epms_prov_npi, source_system_code, epms_user_id, epms_prov_last_name, epms_prov_first_name, epms_prov_enabled_date, epms_prov_active_flag, dw_last_update_date_time)
SELECT DISTINCT rc3.im_domain_id,
                rc3.epms_prov_npi,
                rc3.source_system_code,
                rc3.epms_user_id,
                rc3.epms_prov_last_name,
                rc3.epms_prov_first_name,
                rc3.epms_prov_enabled_date,
                rc3.epms_prov_active_flag,
                rc3.dw_last_update_date_time
FROM
  (SELECT DISTINCT imd.im_domain_id,
                   rc2.epms_prov_npi,
                   'R' AS source_system_code,
                   coalesce(upper(ad.ad_account_user_id), '') AS epms_user_id,
                   rc2.epms_prov_last_name,
                   rc2.epms_prov_first_name,
                   rc2.epms_prov_enabled_date,
                   rc2.epms_prov_active_flag,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM -- RCOPIA

     (SELECT rc1.provider_npi AS epms_prov_npi,
             rc1.provider_last_name AS epms_prov_last_name,
             rc1.provider_first_name AS epms_prov_first_name,
             CASE
                 WHEN upper(rtrim(rc1.provider_account_status)) = 'A' THEN 'Y'
                 ELSE 'N'
             END AS epms_prov_active_flag,
             rc1.enabled_date AS epms_prov_enabled_date,
             av.unit_number AS epcs_practice_code
      FROM edwim_staging.rcopia_provider_status_stg AS rc1
      INNER JOIN
        (SELECT SPLIT(av_0.attribute_text, ':')[ORDINAL(1)] AS unit_number,
                SPLIT(av_0.attribute_text, ':')[ORDINAL(2)] AS facility_name
         FROM edwim_base_views.ref_attribute_value AS av_0
         WHERE av_0.attribute_id = 15 ) AS av ON rtrim(rc1.practice_name) = rtrim(av.facility_name)
      WHERE rc1.provider_npi IS NOT NULL ) AS rc2
   INNER JOIN edwim_base_views.ref_im_domain AS imd ON rtrim(rc2.epcs_practice_code) = rtrim(imd.im_domain_name)
   AND upper(rtrim(imd.source_system_code)) = 'R'
   LEFT OUTER JOIN edwim_base_views.ad_account AS ad ON SAFE_CAST(rc2.epms_prov_npi AS NUMERIC) = ad.ad_account_npi
   AND ad.im_domain_id = 2) AS rc3 QUALIFY row_number() OVER (PARTITION BY rc3.im_domain_id,
                                                                           rc3.epms_prov_npi,
                                                                           upper(rc3.source_system_code)
                                                              ORDER BY CASE
                                                                           WHEN upper(rtrim(rc3.epms_prov_active_flag)) = 'Y' THEN 1
                                                                           ELSE 2
                                                                       END) = 1
UNION DISTINCT
SELECT DISTINCT epcs3.im_domain_id,
                epcs3.epms_prov_npi,
                epcs3.source_system_code,
                epcs3.epms_user_id,
                epcs3.epms_prov_last_name,
                epcs3.epms_prov_first_name,
                PARSE_DATE("%Y%m%d", CAST(epcs3.epms_prov_enabled_date AS STRING)) AS epms_prov_enabled_date,
                epcs3.epms_prov_active_flag,
                epcs3.dw_last_update_date_time
FROM
  (SELECT DISTINCT imd.im_domain_id,
                   epcs2.epms_prov_npi,
                   'E' AS source_system_code,
                   coalesce(upper(ad.ad_account_user_id), '') AS epms_user_id,
                   epcs2.epms_prov_last_name,
                   epcs2.epms_prov_first_name,
                   epcs2.epms_prov_enabled_date,
                   epcs2.epms_prov_active_flag,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM -- EPCS

     (SELECT epcs.provider_npi AS epms_prov_npi,
             epcs.provider_last_name AS epms_prov_last_name,
             epcs.provider_first_name AS epms_prov_first_name,
             CASE
                 WHEN upper(trim(coalesce(epcs.provider_facility_status, ''))) = 'ACTIVE' THEN 'Y'
                 ELSE 'N'
             END AS epms_prov_active_flag,
             NULL AS epms_prov_enabled_date,
             av.unit_number AS epcs_practice_code
      FROM edwim_staging.epcs_provider_status_stg AS epcs
      INNER JOIN
        (SELECT SPLIT(av_0.attribute_text, ':')[ORDINAL(1)] AS unit_number,
                SPLIT(av_0.attribute_text, ':')[ORDINAL(2)] AS facility_name
         FROM edwim_base_views.ref_attribute_value AS av_0
         WHERE av_0.attribute_id = 15 ) AS av ON rtrim(epcs.source_name) = rtrim(av.facility_name)
      WHERE epcs.provider_npi IS NOT NULL ) AS epcs2
   INNER JOIN edwim_base_views.ref_im_domain AS imd ON rtrim(epcs2.epcs_practice_code) = rtrim(imd.im_domain_name)
   AND upper(rtrim(imd.source_system_code)) = 'E'
   LEFT OUTER JOIN edwim_base_views.ad_account AS ad ON SAFE_CAST(epcs2.epms_prov_npi AS NUMERIC) = ad.ad_account_npi
   AND ad.im_domain_id = 2) AS epcs3 QUALIFY row_number() OVER (PARTITION BY epcs3.im_domain_id,
                                                                             epcs3.epms_prov_npi,
                                                                             upper(epcs3.source_system_code)
                                                                ORDER BY CASE
                                                                             WHEN upper(rtrim(epcs3.epms_prov_active_flag)) = 'Y' THEN 1
                                                                             ELSE 2
                                                                         END) = 1;


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT im_domain_id,
             epms_prov_npi,
             source_system_code
      FROM `hca-hin-dev-cur-comp`.edwim.epms_provider_status
      GROUP BY im_domain_id,
               epms_prov_npi,
               source_system_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-comp`.edwim.epms_provider_status');

ELSE
COMMIT TRANSACTION;

END IF;

END