DECLARE DUP_COUNT INT64;

/*========================================================================
*                                    Hospital Corporation of America
*  This software is proprietary and a trade secret of HCA Information Technology & Services
*  No part of this work may reproduced or used in any form or by any means without written
*  permission of of HCA Information Technology & Services
*
* EPMS_Activity_Ins      Provisioning User Access Controls        Version 1.0        04/18/2018
*-------------------------------------------------------------------------------------------------------------------------------------
*                                                  R E V I S I O N S
*-------------------------------------------------------------------------------------------------------------------------------------
*		2020-11-23 - YDF - Changed Cactus_Privileged_Provider_Sw  to include Applicant facility status.
*		2020-12-03 - YDF - Changed Cactus_Privileged_Provider_Sw to include Applicant facility status,
*                          where Medical Staff Category = Active, Associate/Affiliate, Consulting, Courtesy, Privileges Without Membership.
*       2020-12-16 - YDF - Modified Cactus facility status subquery to ensure that providers privileged in either Coliseum or Coliseum North
*                          are excluded from disablement.
*==========================================================================*/ BEGIN TRUNCATE TABLE edwim.epms_provider_activity;

BEGIN TRANSACTION;


INSERT INTO edwim.epms_provider_activity (im_domain_id, epms_user_id, esaf_activity_date, source_system_code, epms_prov_npi, cactus_privileged_provider_sw, cactus_active_dea_license_sw, mt_active_provider_sw, mt_active_txf_provider_sw, dw_last_update_date_time)
SELECT ep2.im_domain_id,
       ep2.epms_user_id,
       ep2.esaf_activity_date,
       ep2.source_system_code,
       ep2.epms_prov_npi,
       ep2.cactus_privileged_provider_sw,
       ep2.cactus_active_dea_license_sw,
       ep2.mt_active_provider_sw,
       ep2.mt_active_txf_provider_sw,
       ep2.dw_last_update_date_time
FROM
  (SELECT ep.im_domain_id,
          ep.epms_user_id,
          current_date() AS esaf_activity_date,
          ep.source_system_code,
          ep.epms_prov_npi,
          CASE
              WHEN upper(rtrim(ep.source_system_code)) = 'R'
                   AND rtrim(cfs.fac_category) IN(-- WHEN  (MT3.PROVIDER_TYPE_FCLT = '1hcaResdnt' AND CFS.FAC_Status = 'Applicant')
 -- 		THEN 1
 -- WHEN  (MT3.PROVIDER_TYPE_FCLT = '1hcaFellow'  AND CFS.FAC_Status = 'Applicant')
 -- 		THEN 1
 'Active',
 'Associate/Affiliate',
 'Consulting',
 'Courtesy',
 'Privileges without Membership')
                   AND rtrim(cfs.fac_status) IN('Applicant',
                                                'Suspend - Controlled Drugs')
                   AND cfs.facility_effective_to_date >= current_date() THEN 1
              ELSE cfs.cactus_privileged_provider_sw
          END AS cactus_privileged_provider_sw,
          CASE
              WHEN upper(rtrim(ep.source_system_code)) = 'E'
                   AND CAST(`hca-hin-dev-cur-pub`.bqutil_fns.cw_td_normalize_number(dea.cactus_active_dea_license_sw) AS FLOAT64) = 1 THEN 1
              ELSE 0
          END AS cactus_active_dea_license_sw,
          CASE upper(rtrim(mt3.provider_type_fclt))
              WHEN '1HCARESDNT' THEN 1
              WHEN '1HCAFELLOW' THEN 1
              ELSE 0
          END AS mt_active_provider_sw,
          CASE
              WHEN upper(rtrim(mt3.network_mnemonic)) = 'TXF'
                   AND upper(rtrim(mt3.provider_type_fclt)) = '2HCAACTIVE' THEN 1
              WHEN upper(rtrim(mt3.network_mnemonic)) = 'TXF'
                   AND upper(rtrim(mt3.provider_type_fclt)) = '2HCAPRVNOM' THEN 1
              WHEN upper(rtrim(mt3.network_mnemonic)) = 'TXF'
                   AND upper(rtrim(mt3.provider_type_fclt)) = '2HCAASOCAF' THEN 1
              WHEN upper(rtrim(mt3.network_mnemonic)) = 'TXF'
                   AND upper(rtrim(mt3.provider_type_fclt)) = '2HCACOURTS' THEN 1
              WHEN upper(rtrim(mt3.network_mnemonic)) = 'TXF'
                   AND upper(rtrim(mt3.provider_type_fclt)) = '2HCACONSUL' THEN 1
              WHEN upper(rtrim(mt3.network_mnemonic)) = 'TXF'
                   AND upper(rtrim(mt3.provider_type_fclt)) = '1HCARESDNT' THEN 1
              WHEN upper(rtrim(mt3.network_mnemonic)) = 'TXF'
                   AND upper(rtrim(mt3.provider_type_fclt)) = '1HCAFELLOW' THEN 1
              WHEN upper(rtrim(mt3.network_mnemonic)) = 'TXF' THEN 0
              ELSE NULL
          END AS mt_active_txf_provider_sw, -- WHEN (MT3.NETWORK_MNEMONIC = 'TXF' AND MT3.PROVIDER_TYPE_FCLT = '2hcaAmbul')
 -- 	THEN 1
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT im.im_domain_name AS epms_prac_code,
             epms.im_domain_id,
             st.state_code,
             epms.epms_prov_npi,
             epms.source_system_code,
             epms.epms_user_id,
             epms.epms_prov_last_name,
             epms.epms_prov_first_name,
             epms.epms_prov_enabled_date,
             epms.epms_prov_active_flag,
             epms.dw_last_update_date_time
      FROM edwim_base_views.epms_provider_status AS epms
      INNER JOIN edwim_base_views.ref_im_domain AS im ON epms.im_domain_id = im.im_domain_id
      INNER JOIN
        (SELECT DISTINCT company_master.unit_num,
                         company_master.state_code
         FROM edwim_views.company_master
         WHERE rtrim(coalesce(company_master.state_code, '')) <> '' QUALIFY row_number() OVER (PARTITION BY company_master.unit_num,
                                                                                                            company_master.state_code
                                                                                               ORDER BY 2) = 1 ) AS st ON im.im_domain_name = st.unit_num
      WHERE rtrim(coalesce(epms.epms_user_id, '')) <> ''
        AND upper(rtrim(epms.epms_prov_active_flag)) = 'Y' ) AS ep
   LEFT OUTER JOIN /*LEFT JOIN ( --CACTUS FACILITY STATUS

                										SELECT

                												   	(CASE 	WHEN CMap.COID_Shared IS NULL AND CP.COID in ('08158','08165')
                															THEN '08158'
                															ELSE COALESCE(CMap.COID_Shared, CP.COID)
                													 END) AS COID,
                													(CASE 	WHEN CMap.Shared_Unit_Number IS NULL AND CP.Unit_Number in ('08158','08165')
                															THEN '08158'
                															ELSE COALESCE(CMap.Shared_Unit_Number, CP.Unit_Number)
                													 END) AS Unit_Number,
                													CP.NPI_Number,
                													CP.FAC_Status,
                													CP.FAC_Category,
                													CP.Facility_Effective_To_Date,
                													CASE
                														WHEN CP.FAC_Category IN ('Active','Associate/Affiliate','Consulting','Courtesy','Privileges without Membership')
                																AND CP.FAC_Status IN ('Applicant','Current','Temporary','Suspend - Select','Suspend - Admitting')
                																	AND CP.Facility_Effective_To_Date >= DATE
                																THEN 1
                														WHEN CP.FAC_Category IN ('Active','Associate/Affiliate','Consulting','Courtesy','Privileges without Membership')
                																AND CP.FAC_Status IN ('Applicant')
                																THEN 1
                															ELSE 0
                													END AS Cactus_Privileged_Provider_Sw
                													FROM
                													(
                																	SELECT

                																	 hcp.HCP_NPI AS NPI_Number
                																	,e1.Coid AS COID
                																	,e1.Company_Code
                																	,fac.Unit_Num AS Unit_Number
                																	,f1.Fac_Asgn_Stts_Desc AS FAC_Status
                																	,f2.Asgn_Cat_Desc AS FAC_Category
                																	,e1.HCP_Fac_Asgn_Eff_To_Date AS Facility_Effective_To_Date

                																	FROM   EDWIM_BASE_VIEWS.HCP hcp

                																	INNER JOIN   EDWIM_BASE_VIEWS.HCP_Facility_Assignment e1		-- Level Code E = Entity, A = Dept
                																	ON hcp.HCP_DW_ID = e1.HCP_DW_ID
                																	AND e1.Fac_Asgn_Level_Code = 'E'

                																	LEFT OUTER JOIN EDWIM_BASE_VIEWS.Ref_Facility_Asgn_Status f1 ON e1.Fac_Asgn_Stts_Sid = f1.Fac_Asgn_Stts_Sid
                																	LEFT OUTER JOIN EDWIM_BASE_VIEWS.Ref_Assignment_Category f2 ON e1.Asgn_Cat_Sid = f2.Asgn_Cat_Sid
                																	LEFT OUTER JOIN  `hca-hin-dev-cur-comp`.auth_base_views.Fact_Facility fac ON e1.Coid = fac.Coid AND fac.Company_Code = e1.Company_Code
                																	WHERE Coalesce(hcp.HCP_NPI,0) <> 0
                																	AND Trim(Coalesce(e1.COID,'')) NOT = ''
                																	GROUP BY 1, 2, 3, 4, 5, 6, 7
                													)  CP
                													LEFT JOIN
                													(
                																	SELECT

                																	Substr(Attribute_Text, 1, 5) AS COID,
                																	Substr(Attribute_Text, 7, 5) AS COID_Shared,
                																	com.Unit_Num  AS Shared_Unit_Number

                																	FROM  EDWIM_BASE_VIEWS.Ref_Attribute_Value rav
                																	JOIN EDWIM_VIEWS.Company_Master com
                																	ON Substr(Attribute_Text, 7, 5) = com.Coid

                																	WHERE rav.Attribute_Id = 14
                																	AND rav.Attribute_Active_Ind = 'Y'
                													)CMap
                													ON	CP.COID = Cmap.COID

                													QUALIFY Row_Number() Over
                															(PARTITION BY
                																(CASE 	WHEN CMap.COID_Shared IS NULL AND CP.COID in ('08158','08165') THEN '08158'
                																 		ELSE COALESCE(CMap.COID_Shared, CP.COID)
                													 			 END),	CP.NPI_Number
                															 ORDER BY Cactus_Privileged_Provider_Sw DESC,
                															 		  (CASE WHEN (	CP.FAC_Category IN ('Active','Associate/Affiliate','Consulting','Courtesy','Privileges without Membership')
                											  										AND CP.FAC_Status IN ( 'Applicant','Suspend - Controlled Drugs')
                											  										AND CP.Facility_Effective_To_Date >= DATE
                																				 ) THEN 1 ELSE 0 END) DESC
                															) = 1

                								)CFS
                							ON EP.EPMS_Prov_NPI = CFS.NPI_Number
                							--AND EP.EPMS_Prac_Code =  CASE WHEN CFS.Unit_Number = EP.EPMS_Prac_Code THEN CFS.Unit_Number ELSE CFS.Shared_Unit_Number END
                							AND EP.EPMS_Prac_Code =  CFS.Unit_Number*/ -- CACTUS FACILITY STATUS

     (SELECT CASE
                 WHEN cmap.coid_shared IS NULL
                      AND rtrim(cp.coid) IN('08158',
                                            '08165') THEN '08158'
                 ELSE coalesce(cmap.coid_shared, cp.coid)
             END AS coid,
             CASE
                 WHEN cmap.shared_unit_number IS NULL
                      AND rtrim(cp.unit_number) IN('08158',
                                                   '08165') THEN '08158'
                 ELSE coalesce(cmap.shared_unit_number, cp.unit_number)
             END AS unit_number,
             cp.npi_number,
             cp.fac_status,
             cp.fac_category,
             cp.facility_effective_to_date,
             CASE
                 WHEN rtrim(cp.fac_category) IN('Active',
                                                'Associate/Affiliate',
                                                'Consulting',
                                                'Courtesy',
                                                'Privileges without Membership')
                      AND rtrim(cp.fac_status) IN('Applicant',
                                                  'Current',
                                                  'Temporary',
                                                  'Suspend - Select',
                                                  'Suspend - Admitting')
                      AND cp.facility_effective_to_date >= current_date() THEN 1
                 WHEN rtrim(cp.fac_category) IN('Active',
                                                'Associate/Affiliate',
                                                'Consulting',
                                                'Courtesy',
                                                'Privileges without Membership')
                      AND rtrim(cp.fac_status) IN('Applicant') THEN 1
                 ELSE 0
             END AS cactus_privileged_provider_sw
      FROM
        (SELECT hcp.hcp_npi AS npi_number,
                e1.coid AS coid,
                e1.company_code,
                fac.unit_num AS unit_number,
                f1.fac_asgn_stts_desc AS fac_status,
                f2.asgn_cat_desc AS fac_category,
                e1.hcp_fac_asgn_eff_to_date AS facility_effective_to_date
         FROM edwim_base_views.hcp
         INNER JOIN edwim_base_views.hcp_facility_assignment AS e1 ON hcp.hcp_dw_id = e1.hcp_dw_id
         AND upper(rtrim(e1.fac_asgn_level_code)) = 'E'
         LEFT OUTER JOIN --  Level Code E = Entity, A = Dept
 edwim_base_views.ref_facility_asgn_status AS f1 ON e1.fac_asgn_stts_sid = f1.fac_asgn_stts_sid
         LEFT OUTER JOIN edwim_base_views.ref_assignment_category AS f2 ON e1.asgn_cat_sid = f2.asgn_cat_sid
         LEFT OUTER JOIN `hca-hin-dev-cur-comp`.auth_base_views.fact_facility AS fac ON e1.coid = fac.coid
         AND fac.company_code = e1.company_code
         WHERE coalesce(hcp.hcp_npi, 0) <> 0
           AND trim(coalesce(e1.coid, '')) <> ''
         GROUP BY 1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7) AS cp
      LEFT OUTER JOIN
        (SELECT substr(attribute_text, 1, 5) AS coid,
                substr(attribute_text, 7, 5) AS coid_shared,
                com.unit_num AS shared_unit_number
         FROM edwim_base_views.ref_attribute_value AS rav
         INNER JOIN edwim_views.company_master AS com ON rtrim(substr(attribute_text, 7, 5)) = rtrim(com.coid)
         WHERE rav.attribute_id = 14
           AND upper(rtrim(rav.attribute_active_ind)) = 'Y' ) AS cmap ON rtrim(cp.coid) = rtrim(cmap.coid) QUALIFY row_number() OVER (PARTITION BY coid,
                                                                                                                                                   cp.npi_number
                                                                                                                                      ORDER BY cactus_privileged_provider_sw DESC, CASE
                                                                                                                                                                                       WHEN rtrim(cp.fac_category) IN('Active',
                                                                                                                                                                                                                      'Associate/Affiliate',
                                                                                                                                                                                                                      'Consulting',
                                                                                                                                                                                                                      'Courtesy',
                                                                                                                                                                                                                      'Privileges without Membership')
                                                                                                                                                                                            AND rtrim(cp.fac_status) IN('Applicant',
                                                                                                                                                                                                                        'Suspend - Controlled Drugs')
                                                                                                                                                                                            AND cp.facility_effective_to_date >= current_date() THEN 1
                                                                                                                                                                                       ELSE 0
                                                                                                                                                                                   END DESC) = 1) AS cfs ON SAFE_CAST(ep.epms_prov_npi AS NUMERIC) = cfs.npi_number
   AND upper(rtrim(ep.epms_prac_code)) = upper(rtrim(cfs.unit_number))
   LEFT OUTER JOIN -- DEA LICENSE FROM CACTUS

     (SELECT dea_lic.npi_number,
             dea_lic.dea_license_state_code,
             dea_lic.dea_license_status_desc,
             dea_lic.dea_license_persistent_verification,
             dea_lic.dea_license_expiration_date,
             '1' AS cactus_active_dea_license_sw
      FROM
        (SELECT hcp.hcp_npi AS npi_number, -- d1.HCP_DW_Id,
 d1.hcp_licn_persistent_vfy AS dea_license_persistent_verification, -- e2.Coid,
 d1.hcp_licn_number AS dea_license_number,
 ad.state_code AS dea_license_state_code,
 CASE
     WHEN d1.hcp_licn_expiration_date IS NOT NULL
          AND d1.hcp_licn_verbal_expr_date IS NOT NULL THEN CASE
                                                                WHEN d1.hcp_licn_verbal_expr_date > d1.hcp_licn_expiration_date THEN d1.hcp_licn_verbal_expr_date
                                                                ELSE d1.hcp_licn_expiration_date
                                                            END
     ELSE coalesce(d1.hcp_licn_expiration_date, d1.hcp_licn_verbal_expr_date)
 END AS dea_license_expiration_date, -- Coalesce(d1.HCP_Licn_Expiration_Date,  d1.HCP_Licn_Verbal_Expr_Date) AS Expiration_Date,
 s1.licn_stts_desc AS dea_license_status_desc,
 d2.licn_type_desc AS license_type_desc,
 trim(rtrim(concat(CASE
                       WHEN upper(trim(coalesce(d1.licn_schd_1_ind, ''))) <> 'Y'
                            OR upper(trim(coalesce(d1.licn_schd_1l_ind, ''))) <> 'Y' THEN '1|'
                       ELSE ''
                   END, CASE
                            WHEN upper(trim(coalesce(d1.licn_schd_2_ind, ''))) <> 'Y'
                                 OR upper(trim(coalesce(d1.licn_schd_2n_ind, ''))) <> 'Y' THEN '2|'
                            ELSE ''
                        END, CASE
                                 WHEN upper(trim(coalesce(d1.licn_schd_3_ind, ''))) <> 'Y'
                                      OR upper(trim(coalesce(d1.licn_schd_3n_ind, ''))) <> 'Y' THEN '3|'
                                 ELSE ''
                             END, CASE
                                      WHEN upper(trim(coalesce(d1.licn_schd_4_ind, ''))) <> 'Y' THEN '4|'
                                      ELSE ''
                                  END, CASE
                                           WHEN upper(trim(coalesce(d1.licn_schd_5_ind, ''))) <> 'Y' THEN '5|'
                                           ELSE ''
                                       END, '6|'), '|')) AS cactus_dea_exclusion_schedule,
 d1.hcp_licn_awarded_date AS license_award_date
         FROM edwim_base_views.hcp_license AS d1
         INNER JOIN edwim_base_views.hcp ON hcp.hcp_dw_id = d1.hcp_dw_id
         INNER JOIN edwim_base_views.ref_license_status AS s1 ON d1.licn_stts_sid = s1.licn_stts_sid
         INNER JOIN edwim_base_views.ref_license_type AS d2 ON d1.licn_type_sid = d2.licn_type_sid
         INNER JOIN edwim_base_views.hcp_facility_assignment AS e2 ON d1.hcp_dw_id = e2.hcp_dw_id
         INNER JOIN `hca-hin-dev-cur-comp`.auth_base_views.facility_address AS ad ON e2.coid = ad.coid
         AND upper(rtrim(ad.address_type_code)) = 'PH'
         WHERE upper(rtrim(d1.active_dw_ind)) = 'Y'
           AND upper(rtrim(d1.source_system_code)) = 'K'
           AND NOT rtrim(d1.state_code) = ''
           AND rtrim(coalesce(d1.hcp_licn_number, '')) <> ''
           AND ad.state_code = d1.state_code
           AND rtrim(coalesce(d1.hcp_licn_number, '')) <> ''
           AND upper(trim(coalesce(d2.licn_type_desc, ''))) = 'DEA CERTIFICATE'
           AND upper(trim(coalesce(d1.hcp_licn_persistent_vfy, ''))) = 'Y'
           AND upper(trim(coalesce(s1.licn_stts_desc, ''))) IN('ACTIVE',
                                                               'TEMPORARY PERMIT',
                                                               'ACTIVE MILITARY',
                                                               'CPC - UNDER REVIEW',
                                                               'EXEMPT',
                                                               'EXPIRED',
                                                               'LIMITED',
                                                               'PENDING',
                                                               'RESTRICTED',
                                                               'REVOKED',
                                                               'SUSPENDED')
           AND coalesce(hcp.hcp_npi, 0) <> 0 QUALIFY row_number() OVER (PARTITION BY npi_number,
                                                                                     d1.state_code
                                                                        ORDER BY CASE
                                                                                     WHEN upper(trim(coalesce(dea_license_persistent_verification, ''))) = 'Y' THEN 1
                                                                                     ELSE 0
                                                                                 END DESC, dea_license_expiration_date DESC, coalesce(license_award_date, parse_date('%m/%d/%Y', '01/01/1950')) DESC) = 1 ) AS dea_lic
      WHERE dea_lic.dea_license_expiration_date >= date_sub(current_date(), interval 3 DAY) ) AS dea ON SAFE_CAST(ep.epms_prov_npi AS NUMERIC) = dea.npi_number
   AND ep.state_code = dea.dea_license_state_code
   LEFT OUTER JOIN -- AND EP.EPMS_Prac_Code =  CASE WHEN DEA.Unit_Number = EP.EPMS_Prac_Code THEN DEA.Unit_Number ELSE DEA.Shared_Unit_Number END
 -- MEDITECH

     (SELECT CAST(/* expression of unknown or erroneous type */ prc.prctnr_ssk_cs AS STRING) AS provider_mnemonic,
             substr(concat(trim(coalesce(prc.src_sys_actv_ind, '')), ' '), 1, 1) AS active_ind,
             CAST(/* expression of unknown or erroneous type */ prc.ntwk_ssk_cs AS STRING) AS network_mnemonic,
             coalesce(bymnemonic.role_plyr_sk, bynpi.role_plyr_sk) AS role_plyr_sk,
             CAST(/* expression of unknown or erroneous type */ prc.prctnr_role_cd AS STRING) AS provider_type,
             CAST(/* expression of unknown or erroneous type */ prc.natnl_prvdr_id AS STRING) AS npi_number,
             fclt.fclt_ssk_cs,
             fclt.coid,
             fclt.provider_type_fclt,
             fclt.unit_number
      FROM edwim_base_views.prctnr_ent AS prc
      INNER JOIN
        (SELECT CAST(/* expression of unknown or erroneous type */ fac.ntwk_ssk_cs AS STRING) AS ntwk_ssk_cs,
                CAST(/* expression of unknown or erroneous type */ fac.fclt_ssk_cs AS STRING) AS fclt_ssk_cs,
                CAST(/* expression of unknown or erroneous type */ fac.prctnr_ssk_cs AS STRING) AS prctnr_ssk_cs,
                CAST(/* expression of unknown or erroneous type */ fac.coid AS STRING) AS coid,
                CAST(/* expression of unknown or erroneous type */ fac.prctnr_fclt_role_cd AS STRING) AS provider_type_fclt,
                comp.unit_num AS unit_number
         FROM edwim_base_views.prctnr_fclt_ent AS fac
         INNER JOIN edwim_views.company_master AS comp ON fac.company_code = comp.company_code
         AND fac.coid = comp.coid
         WHERE fac.vld_to_ts = '9999-12-31 00:00:00' QUALIFY row_number() OVER (PARTITION BY upper(fac.ntwk_ssk_cs),
                                                                                             upper(fac.fclt_ssk_cs),
                                                                                             upper(fac.prctnr_ssk_cs)
                                                                                ORDER BY fac.src_lst_upd_ts DESC, fac.vld_to_ts DESC, fac.vld_fr_ts DESC) = 1 ) AS fclt ON upper(rtrim(CAST(/* expression of unknown or erroneous type */ prc.ntwk_ssk_cs AS STRING))) = upper(rtrim(fclt.ntwk_ssk_cs))
      AND upper(rtrim(CAST(/* expression of unknown or erroneous type */ prc.prctnr_ssk_cs AS STRING))) = upper(rtrim(fclt.prctnr_ssk_cs))
      LEFT OUTER JOIN --  ##############################################################
 --  ##########    SEARCH FOR ROLE_PLYR_SK BY NPI
 --  ##############################################################
 --  Search By NPI

        (SELECT prc_0.role_plyr_sk,
                fclt_0.coid,
                ROUND(CAST(`hca-hin-dev-cur-pub`.bqutil_fns.cw_td_normalize_number(trim(npi.id_txt)) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS npi_number
         FROM edwim_base_views.prctnr_dtl AS prc_0
         INNER JOIN edwim_base_views.prctnr_fclt_dtl AS fclt_0 ON prc_0.role_plyr_sk = fclt_0.role_plyr_sk
         AND fclt_0.vld_to_ts = '9999-12-31 00:00:00'
         AND rtrim(fclt_0.src_sys_ref_cd) IN('MIS_PROVIDER')
         AND upper(rtrim(fclt_0.company_code)) = 'H'
         INNER JOIN --  NPI

           (SELECT hcp.role_plyr_sk,
                   hcp.id_txt,
                   hcp.registn_type_ref_cd,
                   nppes.provider_last_name,
                   nppes.provider_first_name,
                   nppes.provider_middle_name,
                   nppes.entity_type_code,
                   CASE nppes.entity_type_code
                       WHEN 1 THEN 'Individual'
                       WHEN 2 THEN 'Organization'
                       WHEN 3 THEN 'Deactivated'
                       ELSE NULL
                   END AS npi_type_desc
            FROM edwim_base_views.prctnr_role_idfn AS hcp
            INNER JOIN `hca-hin-dev-cur-comp`.auth_base_views.nppes_provider AS nppes ON trim(hcp.id_txt) = rtrim(CAST(/* expression of unknown or erroneous type */ nppes.npi AS STRING))
            WHERE rtrim(hcp.src_sys_ref_cd) IN('MIS_PROVIDER',
                                               'MIS')
              AND hcp.vld_to_ts = '9999-12-31 00:00:00'
              AND upper(rtrim(hcp.registn_type_ref_cd)) = 'NPI' QUALIFY row_number() OVER (PARTITION BY hcp.role_plyr_sk
                                                                                           ORDER BY hcp.vld_to_ts DESC, hcp.vld_fr_ts DESC) = 1 ) AS npi ON npi.role_plyr_sk = prc_0.role_plyr_sk QUALIFY row_number() OVER (PARTITION BY fclt_0.coid,
                                                                                                                                                                                                                                          trim(npi.id_txt)
                                                                                                                                                                                                                             ORDER BY fclt_0.vld_to_ts DESC, fclt_0.vld_fr_ts DESC) = 1) AS bynpi ON upper(rtrim(fclt.coid)) = upper(rtrim(bynpi.coid))
      AND prc.natnl_prvdr_id = bynpi.npi_number
      LEFT OUTER JOIN -- ON Upper(Cast(prc.NTWK_SSK_CS AS CHAR(4))) = byNPI.Network_Mnemonic
 --  ##############################################################
 --  ##########    SEARCH FOR ROLE_PLYR_SK BY PROVIDER MNEMONIC
 --  ##############################################################
 --  Search By Provider Mnemonic

        (SELECT prc_0.role_plyr_sk, -- cl.NETWORK_MNEMONIC_CS AS Network_Mnemonic,
 fclt_0.coid,
 mnem.provider_mnemonic AS provider_mnemonic
         FROM edwim_base_views.prctnr_dtl AS prc_0
         INNER JOIN edwim_base_views.prctnr_fclt_dtl AS fclt_0 ON prc_0.role_plyr_sk = fclt_0.role_plyr_sk
         AND fclt_0.vld_to_ts = '9999-12-31 00:00:00'
         AND rtrim(fclt_0.src_sys_ref_cd) IN('MIS_PROVIDER')
         AND upper(rtrim(fclt_0.company_code)) = 'H'
         INNER JOIN --  Provider Mnemonic

           (SELECT prctnr_role_idfn.role_plyr_sk,
                   prctnr_role_idfn.id_txt AS provider_mnemonic,
                   prctnr_role_idfn.registn_type_ref_cd
            FROM edwim_base_views.prctnr_role_idfn
            WHERE rtrim(prctnr_role_idfn.src_sys_ref_cd) IN('MIS_PROVIDER',
                                                            'MIS')
              AND prctnr_role_idfn.vld_to_ts = '9999-12-31 00:00:00'
              AND upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'MNEMONIC' QUALIFY row_number() OVER (PARTITION BY role_plyr_sk
                                                                                                             ORDER BY prctnr_role_idfn.vld_to_ts DESC, prctnr_role_idfn.vld_fr_ts DESC) = 1 ) AS mnem ON mnem.role_plyr_sk = fclt_0.role_plyr_sk QUALIFY row_number() OVER (PARTITION BY fclt_0.coid,
                                                                                                                                                                                                                                                                                         provider_mnemonic
                                                                                                                                                                                                                                                                            ORDER BY fclt_0.vld_to_ts DESC, fclt_0.vld_fr_ts DESC) = 1) AS bymnemonic ON upper(rtrim(fclt.coid)) = upper(rtrim(bymnemonic.coid))
      AND upper(rtrim(fclt.prctnr_ssk_cs)) = upper(rtrim(bymnemonic.provider_mnemonic))
      WHERE prc.vld_to_ts = '9999-12-31 00:00:00'
        AND NOT (bymnemonic.role_plyr_sk IS NULL
                 AND bynpi.role_plyr_sk IS NULL)
        AND upper(substr(concat(trim(coalesce(prc.src_sys_actv_ind, '')), ' '), 1, 1)) = 'Y'
        AND rtrim(CAST(/* expression of unknown or erroneous type */ prc.natnl_prvdr_id AS STRING)) <> '' QUALIFY row_number() OVER (PARTITION BY upper(fclt.coid),
                                                                                                                                                  upper(fclt.fclt_ssk_cs),
                                                                                                                                                  bynpi.npi_number
                                                                                                                                     ORDER BY prc.vld_to_ts DESC, CASE
                                                                                                                                                                      WHEN upper(rtrim(fclt.provider_type_fclt)) IN(-- QUALIFY Row_Number() Over(PARTITION BY fclt.COID, fclt.FCLT_SSK_CS, Coalesce(byMnemonic.Role_Plyr_Sk, byNPI.Role_Plyr_Sk) ORDER BY prc.SRC_LST_UPD_TS DESC, prc.VLD_TO_TS DESC, prc.VLD_FR_TS DESC) = 1
 '1HCARESDNT',
 '1HCAFELLOW') THEN 1
                                                                                                                                                                      WHEN upper(rtrim(network_mnemonic)) = 'TXF'
                                                                                                                                                                           AND upper(rtrim(fclt.provider_type_fclt)) IN('2HCAACTIVE',
                                                                                                                                                                                                                        '2HCAPRVNOM',
                                                                                                                                                                                                                        '2HCAASOCAF',
                                                                                                                                                                                                                        '2HCACOURTS',
                                                                                                                                                                                                                        '2HCACONSUL') THEN 2
                                                                                                                                                                      ELSE 3
                                                                                                                                                                  END,
                                                                                                                                                                  prc.vld_fr_ts DESC) = 1 ) AS mt3 ON upper(rtrim(ep.epms_prov_npi)) = upper(trim(coalesce(mt3.npi_number, '')))
   AND upper(rtrim(ep.epms_prac_code)) = upper(rtrim(CASE
                                                         WHEN mt3.unit_number = ep.epms_prac_code THEN mt3.unit_number
                                                         ELSE mt3.coid
                                                     END))) AS ep2 QUALIFY row_number() OVER (PARTITION BY ep2.im_domain_id,
                                                                                                           ep2.epms_user_id,
                                                                                                           ep2.esaf_activity_date,
                                                                                                           ep2.source_system_code
                                                                                              ORDER BY CASE 1
                                                                                                           WHEN ep2.cactus_privileged_provider_sw THEN 1
                                                                                                           WHEN ep2.mt_active_provider_sw THEN 2
                                                                                                           ELSE 3
                                                                                                       END) = 1;


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT im_domain_id,
             epms_user_id,
             esaf_activity_date,
             source_system_code
      FROM `hca-hin-dev-cur-comp`.edwim.epms_provider_activity
      GROUP BY im_domain_id,
               epms_user_id,
               esaf_activity_date,
               source_system_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-comp`.edwim.epms_provider_activity');

ELSE
COMMIT TRANSACTION;

END IF;

END;

-- REMOVE DUPS