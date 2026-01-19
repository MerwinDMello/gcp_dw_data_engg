-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/case_mgmt_denial_analysis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.case_mgmt_denial_analysis AS SELECT
    denial_eom.pe_date,
    denial_eom.discharge_date,
    denial_eom.denial_status_code,
    denial_eom.iplan_id,
    denial_eom.service_code,
    denial_eom.patient_type_code,
    denial_eom.patient_financial_class_code,
    denial_eom.payor_financial_class_code,
    denial_eom.coid,
    denial_eom.unit_num,
    denial_eom.pat_acct_num,
    substr(dim_denial_code.denial_code_alias_name, 6, 25) AS denial_code_alias_name,
    max(major_payor.major_payor_name) AS major_payor_name,
    max(coalesce(chois_product_line.chois_product_line_desc, 'DRG Not Assigned')) AS chois_product_line_desc,
    ROUND(fact_patient.financial_class_code, 0, 'ROUND_HALF_EVEN') AS fp_financial_class_code,
    max(CASE
       fact_patient.financial_class_code
      WHEN 1 THEN 'MEDICARE - PPS'
      WHEN 2 THEN 'MEDICARE - DPU'
      WHEN 3 THEN 'MEDICAID'
      WHEN 4 THEN 'WORKERS COMP'
      WHEN 5 THEN 'COMMERCIAL'
      WHEN 6 THEN 'CHAMPUS'
      WHEN 7 THEN 'HMO'
      WHEN 8 THEN 'PPO'
      WHEN 9 THEN 'MANAGED CARE MEDICAID'
      WHEN 10 THEN 'FEDERAL'
      WHEN 11 THEN 'STATE NON MCAID LOCL GOV'
      WHEN 12 THEN 'MANAGED CARE MEDICARE'
      WHEN 13 THEN 'BLUE CROSS/COST'
      WHEN 14 THEN 'EXCHANGES'
      WHEN 15 THEN 'CHARITY'
      WHEN 99 THEN 'SELF PAY'
      WHEN 0 THEN 'UNASSIGNED'
      ELSE 'UNKNOWN PAYOR CLASS'
    END) AS fp_financial_class_code_desc,
    fact_facility.coid_name,
    max(bi_major_payor.major_payor_name) AS bi_major_payor_name,
    max(ref_cc_root_cause.cc_root_cause_desc) AS cc_root_cause_desc,
    denial_eom.cc_root_cause_id,
    ROUND(fact_patient.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    count(DISTINCT fact_patient.patient_dw_id) AS pat_cnt,
    denial_eom.iplan_insurance_order_num,
    max(CASE
       denial_eom.patient_financial_class_code
      WHEN 1 THEN 'MEDICARE - PPS'
      WHEN 2 THEN 'MEDICARE - DPU'
      WHEN 3 THEN 'MEDICAID'
      WHEN 4 THEN 'WORKERS COMP'
      WHEN 5 THEN 'COMMERCIAL'
      WHEN 6 THEN 'CHAMPUS'
      WHEN 7 THEN 'HMO'
      WHEN 8 THEN 'PPO'
      WHEN 9 THEN 'MANAGED CARE MEDICAID'
      WHEN 10 THEN 'FEDERAL'
      WHEN 11 THEN 'STATE NON MCAID LOCL GOV'
      WHEN 12 THEN 'MANAGED CARE MEDICARE'
      WHEN 13 THEN 'BLUE CROSS/COST'
      WHEN 14 THEN 'EXCHANGES'
      WHEN 15 THEN 'CHARITY'
      WHEN 99 THEN 'SELF PAY'
      WHEN 0 THEN 'UNASSIGNED'
      ELSE 'UNKNOWN PAYOR CLASS'
    END) AS dem_financial_class_desc,
    max(CASE
      WHEN denial_eom.denial_status_code IN(
        'WZ', 'XZ', 'YZ', 'W0', 'X0', 'Y0', 'WM', 'XM', 'YM'
      ) THEN 'All Coding Denial'
      WHEN denial_eom.denial_status_code IN(
        'WW', 'XW', 'YW', 'WR', 'XR', 'YR', 'W2', 'X2', 'Y2', 'W7', 'X7', 'Y7', 'W4', 'X4', 'Y4', 'W1', 'X1', 'Y1', 'W6', 'X6', 'Y6', 'W3', 'X3', 'Y3', 'W5', 'X5', 'Y5'
      ) THEN 'Information Requested/Other Technical Denial'
      WHEN denial_eom.denial_status_code IN(
        'WC', 'XC', 'YC', 'WT', 'XT', 'YT', 'WH', 'XH', 'YH', 'WI', 'XI', 'YI', 'WG', 'XG', 'YG', 'WJ', 'XJ', 'YJ', 'WU', 'XU', 'YU', 'WV', 'XV', 'YV', 'WK', 'XK', 'YK', 'WL', 'XL', 'YL'
      ) THEN 'Medical Necessity'
      WHEN denial_eom.denial_status_code IN(
        'W9', 'X9', 'Y9', 'W8', 'X8', 'Y8', 'WP', 'XP', 'YP', 'WQ', 'XQ', 'YQ', 'WO', 'XO', 'YO', 'WN', 'XN', 'YN', 'WA', 'XA', 'YA', 'WB', 'XB', 'YB', 'WS', 'XS', 'YS', 'WE', 'XE', 'YE', 'WF', 'XF', 'YF', 'WD', 'XD', 'YD'
      ) THEN 'No Auth/Notification'
      WHEN denial_eom.denial_status_code IN(
        '99', '98', '#EMPTY'
      ) THEN 'No Denial Code'
      WHEN denial_eom.denial_status_code IN(
        'WX', 'XX', 'YX', 'WY', 'XY', 'YY'
      ) THEN 'Timely Filing'
      WHEN denial_eom.denial_status_code IN(
        '8H', '8I', '8P'
      ) THEN 'Pending'
      ELSE 'No Denial Code'
    END) AS eom_denial_status_code,
    max(CASE
      WHEN denial_eom.cc_root_cause_id IN(
        '520D      ', '520H      ', '520I      ', '521B      ', '522H      ', '523H      ', '524H      ', '525H      ', '526H      ', 'CDCDH     ', 'CDLAC     ', 'CDOVB     ', 'CDCD      ', 'CDLA      ', 'CDOV      '
      ) THEN 'Coding'
      WHEN denial_eom.cc_root_cause_id IN(
        '250B      ', '250H      ', '260B      ', '260C      ', '340C      ', '340D      ', '340I      ', '340B      ', '340H      ', '340R      ', '350D      ', '350H      ', '350I      ', '365D      ', '380D      ', '380R      ', '415C      ', 'IRMRO     ', 'IRPFB     ', 'IRSFR     ', 'IRMR      ', 'IRPF      ', 'IRSF      '
      ) THEN 'Info Req, Not Provided'
      WHEN denial_eom.cc_root_cause_id IN(
        '550P      ', '550R      ', '550V      ', 'IAREI     ', 'ICEFI     ', 'ICEIR     ', 'ICENR     ', 'ICPAP     ', 'IARE      ', 'ICEF      ', 'ICEI      ', 'ICEN      ', 'ICPA      '
      ) THEN 'I-Plan Update'
      WHEN denial_eom.cc_root_cause_id IN(
        '425D      ', '425P      ', '430C      ', '430D      ', '435I      ', '435D      ', '440C      ', '450C      ', '450D      ', '460C      ', '460D      ', '465C      ', '465D      ', '470D      ', '470I      ', '480B      ', '480C      ', '480D      ', '480I      ', '480R      ', '480V      ', '481I      ', '482C      ', 'MNLDC     ', 'MNMPI     ', 'MNMRC     ', 'MNNAI     ', 'MNSIB     ', 'MNSIC     ', 'MNSIR     ', 'MNTXI     ', 'MNLD      ', 'MNMP      ', 'MNMR      ', 'MNNA      ', 'MNSI      ', 'MNSI      ', 'MNSI      ', 'MNTX      '
      ) THEN 'Medical Necessity'
      WHEN denial_eom.cc_root_cause_id IN(
        '105D      ', '115D      ', '115R      ', '115V      ', '125C      ', '125I      ', '125R      ', '125V      ', '134C      ', '135C      ', '136C      ', '140I      ', '150I      ', '155C      ', '155R      ', '155V      ', '170I      ', 'IAAUC     ', 'IAIPR     ', 'IALIR     ', 'IAOPR     ', 'IAAU      ', 'IAIP      ', 'IALI      ', 'IAOP      '
      ) THEN 'Missing / Invalid Auth or Notification'
      WHEN denial_eom.cc_root_cause_id IN(
        '650A      ', '650B      ', '650C      ', '650I      ', '650U      ', '650V      ', '650P      ', 'IDAPI     ', 'IDHXI     ', 'IDIMI     ', 'IDMNI     ', 'IDNAI     ', 'IDAP      ', 'IDHX      ', 'IDIM      ', 'IDMN      ', 'IDNA      '
      ) THEN 'Inappropriate Denials'
      WHEN denial_eom.cc_root_cause_id IN(
        '530B      ', '530C      ', '530D      ', '530I      ', '530P      ', '530R      ', 'OTOTA     ', 'OTOTB     ', 'OTOTC     ', 'OTOTI     ', 'OTOTO     ', 'OTOTR     ', 'OTTMO     ', 'OTOT     ', 'OTTM      '
      ) THEN 'Other'
      WHEN denial_eom.cc_root_cause_id IN(
        '220A      ', '220B      ', '220C      ', '220H      ', '220V      ', '220R      ', '220U      ', '355P      ', '355I      ', 'TFCCB     ', 'TFOII     ', 'TFRBA     ', 'TFTFO     ', 'TFCC      ', 'TFOI      ', 'TFRB      ', 'TFTF      '
      ) THEN 'Timely Filing'
      WHEN denial_eom.cc_root_cause_id IN(
        'CTICC     ', 'CTPDI     ', 'CTSCC     ', 'CTIC      ', 'CTPD      ', 'CTSC      '
      ) THEN 'Timely Response - Clinicals'
      ELSE 'N/A'
    END) AS eom_cc_root_cause_id,
    max(CASE
      WHEN drv_denial_full_document.patient_dw_id IS NULL THEN 'N'
      WHEN drv_denial_full_document.patient_dw_id IS NOT NULL
       AND drv_denial_full_document.payor_financial_class_code IN(
        1, 2, 15, 99
      ) THEN 'N'
      ELSE 'Y'
    END) AS drv_denial_full_document_ind,
    facility_iplan_eom.payor_name,
    ssc_facility.coid_name AS facility_coid_name,
    substr(eis_denial_disp_dim.denial_disp_alias, 4, 27) AS denial_disp_alias,
    denial_eom.disposition_num,
    fact_patient.calculated_los,
    drv_surgical_case_type.case_type,
    max(CASE
      WHEN drv_partial_denial.patient_dw_id IS NULL THEN 'N'
      ELSE 'Y'
    END) AS drv_partial_denial_ind,
    max(CASE
      WHEN upper(cm_encounter.iq_adm_rev_type_ip_ind) = 'Y'
       OR upper(cm_encounter.iq_adm_rev_type_obs_ind) = 'Y' THEN 'Y'
      ELSE 'N'
    END) AS iq_adm_rev_type_ip_obs_ind,
    max(cm_encounter.iq_adm_rev_type_ip_mn_met_ind) AS iq_adm_rev_type_ip_mn_met_ind,
    max(cm_encounter.iq_adm_rev_type_obs_mn_met_ind) AS iq_adm_rev_type_obs_mn_met_ind,
    max(CASE
      WHEN pa_disposition_ip.patient_dw_id IS NULL THEN 'N'
      ELSE 'Y'
    END) AS pa_disposition_ip_ind,
    max(CASE
      WHEN pa_disposition_obs.patient_dw_id IS NULL THEN 'N'
      ELSE 'Y'
    END) AS pa_disposition_obs_ind,
    max(CASE
      WHEN external_pa_referral.patient_dw_id IS NULL THEN 'N'
      ELSE 'Y'
    END) AS external_pa_referral_ind,
    cm_encounter_payer.midas_principal_payer_auth_num,
    drv_min_cert_begin_date.min_cert_begin_date,
    drv_max_cert_end.cert_end_date,
    max(drv_max_cert_end.hcm_status_cause_name) AS hcm_status_cause_name,
    max(cm_encounter.midas_acct_num) AS midas_acct_num,
    max(format_date('%Y', fact_patient.discharge_date)) AS discharge_year,
    fact_facility.division_name,
    max(coalesce(fact_patient.drg_desc_hcfa, 'DRG Not Assigned')) AS drg_desc_hcfa,
    max(fact_patient.drg_code_hcfa) AS drg_code_hcfa,
    max(fact_patient.admission_type_code) AS admission_type_code,
    diagnosis_related_group.drg_medical_surgical_ind,
    fact_facility.pas_coid,
    denial_eom.new_denial_account_amt,
    fact_patient.admission_date,
    fact_case_mgmt_patient.service_code AS cm_service_code,
    max(ref_cm_location.location_name) AS location_name
  FROM
    `hca-hin-dev-cur-parallon`.edwcm_views.diagnosis_related_group
    RIGHT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.fact_patient ON upper(diagnosis_related_group.reimbursement_group_code) = upper(fact_patient.drg_code_hcfa)
     AND upper(diagnosis_related_group.reimbursement_group_name) = 'M'
     AND fact_patient.discharge_date BETWEEN diagnosis_related_group.reimbursement_group_start_date AND diagnosis_related_group.reimbursement_group_end_date
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.fact_case_mgmt_patient ON fact_patient.patient_dw_id = fact_case_mgmt_patient.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.fact_facility ON upper(fact_facility.coid) = upper(fact_patient.coid)
     AND upper(fact_facility.company_code) = upper(fact_patient.company_code)
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.fact_facility AS ssc_facility ON fact_facility.pas_coid = ssc_facility.coid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter ON fact_patient.patient_dw_id = cm_encounter.patient_dw_id
     AND upper(cm_encounter.active_dw_ind) = 'Y'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_cm_location ON cm_encounter.location_id = ref_cm_location.location_id
     AND upper(ref_cm_location.active_dw_ind) = 'Y'
     AND upper(cm_encounter.active_dw_ind) = 'Y'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer ON cm_encounter_payer.midas_encounter_id = cm_encounter.midas_encounter_id
     AND upper(cm_encounter_payer.active_dw_ind) = 'Y'
     AND upper(cm_encounter.active_dw_ind) = 'Y'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.denial_eom ON denial_eom.patient_dw_id = fact_patient.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.dim_denial_code ON dim_denial_code.denial_code_child = denial_eom.denial_status_code
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.eis_denial_disp_dim ON substr(eis_denial_disp_dim.denial_disp_alias, 1, 2) = denial_eom.disposition_num
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_cc_root_cause ON upper(ref_cc_root_cause.cc_root_cause_code) = upper(denial_eom.cc_root_cause_id)
     AND upper(ref_cc_root_cause.company_code) = 'H'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.bi_major_payor ON denial_eom.coid = bi_major_payor.coid
     AND denial_eom.iplan_id = bi_major_payor.iplan_id
     AND denial_eom.discharge_date BETWEEN bi_major_payor.eff_from_date AND bi_major_payor.eff_to_date
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.facility_iplan_eom ON denial_eom.coid = facility_iplan_eom.coid
     AND denial_eom.iplan_id = facility_iplan_eom.iplan_id
    LEFT OUTER JOIN (
      SELECT
          ar_transaction.patient_dw_id,
          ar_transaction.iplan_id,
          sum(ar_transaction.ar_transaction_amt) AS payment_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_views.ar_transaction
        WHERE upper(ar_transaction.transaction_type_code) = '1'
        GROUP BY 1, 2
    ) AS drv_partial_denial ON denial_eom.patient_dw_id = drv_partial_denial.patient_dw_id
     AND denial_eom.iplan_id = drv_partial_denial.iplan_id
     AND drv_partial_denial.payment_amt > 0
    LEFT OUTER JOIN (
      SELECT
          fact_patient_0.patient_dw_id,
          denial_eom_0.payor_dw_id,
          denial_eom_0.pe_date,
          denial_eom_0.iplan_insurance_order_num,
          denial_eom_0.payor_financial_class_code
        FROM
          (
            SELECT
                a.patient_dw_id,
                a.cert_end_date,
                a.cert_status_id,
                b.hcm_status_cause_name,
                row_number() OVER (PARTITION BY a.patient_dw_id ORDER BY a.encounter_payer_id, a.encounter_payer_cert_hist_id DESC, c.payer_status_id, a.encounter_payer_cert_id) AS row_num
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer_cert_hist AS a
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer_cert AS c ON a.encounter_payer_id = c.encounter_payer_id
                 AND a.encounter_payer_cert_id = c.encounter_payer_cert_id
                 AND upper(a.active_dw_ind) = 'Y'
                 AND upper(c.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_status_cause AS b ON a.cert_status_id = b.hcm_status_cause_id
                 AND upper(b.active_dw_ind) = 'Y'
              WHERE upper(a.active_dw_ind) = 'Y'
               AND (a.patient_dw_id, coalesce(a.cert_end_date, DATE '1900-01-01')) IN(
                SELECT AS STRUCT
                    cm_encounter_payer_cert_hist.patient_dw_id,
                    max(coalesce(cm_encounter_payer_cert_hist_0.cert_end_date, DATE '1900-01-01'))
                  FROM
                    `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer_cert_hist AS cm_encounter_payer_cert_hist_1
                  WHERE upper(cm_encounter_payer_cert_hist_2.active_dw_ind) = 'Y'
                  GROUP BY 1
              )
          ) AS drv_max_cert_end_0
          RIGHT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.fact_patient AS fact_patient_0 ON drv_max_cert_end_0.patient_dw_id = fact_patient_0.patient_dw_id
           AND drv_max_cert_end_0.row_num = 1
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.denial_eom AS denial_eom_0 ON fact_patient_0.patient_dw_id = denial_eom_0.patient_dw_id
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter AS cm_encounter_0 ON fact_patient_0.patient_dw_id = cm_encounter_0.patient_dw_id
           AND upper(cm_encounter_0.active_dw_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer AS cm_encounter_payer_0 ON cm_encounter_payer_0.midas_encounter_id = cm_encounter_0.midas_encounter_id
           AND upper(cm_encounter_payer_0.active_dw_ind) = 'Y'
           AND upper(cm_encounter_0.active_dw_ind) = 'Y'
          LEFT OUTER JOIN (
            SELECT
                a.patient_dw_id,
                min(a.cert_begin_date) AS min_cert_begin_date
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer_cert_hist AS a
              WHERE upper(a.active_dw_ind) = 'Y'
               AND a.cert_begin_date IS NOT NULL
              GROUP BY 1
          ) AS drv_min_cert_begin_date_0 ON fact_patient_0.patient_dw_id = drv_min_cert_begin_date_0.patient_dw_id
        WHERE upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%NPR%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%NR%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%APPROVAL%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%PEND%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%NOT RE%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%DEN%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%SAME%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%TAR ACC%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%RETRO%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%PER %'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%APPSDC%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%NONE%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%NEED%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%NOTRE%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%NOTIF%'
         AND upper(cm_encounter_payer_0.midas_principal_payer_auth_num) NOT LIKE '%DRG%'
         AND cm_encounter_payer_0.midas_principal_payer_auth_num IS NOT NULL
         AND (upper(drv_max_cert_end_0.hcm_status_cause_name) IN(
          'CERT - DRG PAYER', 'CERT - DRG PAYER@'
        )
         OR (upper(drv_max_cert_end_0.hcm_status_cause_name) = 'CERT -  APPROVED'
         OR drv_max_cert_end_0.hcm_status_cause_name IS NULL)
         AND drv_min_cert_begin_date_0.min_cert_begin_date <= (extract(YEAR from fact_patient_0.admission_date) - 1900) * 10000 + extract(MONTH from fact_patient_0.admission_date) * 100 + extract(DAY from fact_patient_0.admission_date)
         AND drv_max_cert_end_0.cert_end_date >= fact_patient_0.discharge_date)
         AND denial_eom_0.payor_financial_class_code NOT IN(
          1, 2, 15, 99
        )
        GROUP BY 1, 2, 3, 4, 5
    ) AS drv_denial_full_document ON denial_eom.patient_dw_id = drv_denial_full_document.patient_dw_id
     AND denial_eom.payor_dw_id = drv_denial_full_document.payor_dw_id
     AND denial_eom.iplan_insurance_order_num = drv_denial_full_document.iplan_insurance_order_num
     AND denial_eom.pe_date = drv_denial_full_document.pe_date
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.major_payor ON fact_patient.major_payor_id_ins1 = major_payor.major_payor_id
     AND fact_patient.masterfacts_schema_id = major_payor.schema_id
    LEFT OUTER JOIN (
      SELECT
          hcr.patient_dw_id
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_views.hcm_conc_rev_conc_rev AS hcrcr
          INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_concurrent_review AS hcr ON hcrcr.concurrent_review_id = hcr.concurrent_review_id
           AND upper(hcrcr.active_dw_ind) = 'Y'
           AND upper(hcr.active_dw_ind) = 'Y'
          INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_disposition AS rhd ON hcrcr.disposition_id = rhd.disposition_id
           AND upper(rhd.active_dw_ind) = 'Y'
        WHERE upper(rhd.hcm_disposition_code) = 'PA19'
        GROUP BY 1
    ) AS pa_disposition_obs ON fact_patient.patient_dw_id = pa_disposition_obs.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          hcr.patient_dw_id
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_views.hcm_conc_rev_conc_rev AS hcrcr
          INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_concurrent_review AS hcr ON hcrcr.concurrent_review_id = hcr.concurrent_review_id
           AND upper(hcrcr.active_dw_ind) = 'Y'
           AND upper(hcr.active_dw_ind) = 'Y'
          INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_disposition AS rhd ON hcrcr.disposition_id = rhd.disposition_id
           AND upper(rhd.active_dw_ind) = 'Y'
        WHERE upper(rhd.hcm_disposition_code) = 'PA17'
        GROUP BY 1
    ) AS pa_disposition_ip ON fact_patient.patient_dw_id = pa_disposition_ip.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          hcr.patient_dw_id
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_views.hcm_conc_rev_conc_rev AS hcrcr
          INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_concurrent_review AS hcr ON hcrcr.concurrent_review_id = hcr.concurrent_review_id
           AND upper(hcrcr.active_dw_ind) = 'Y'
           AND upper(hcr.active_dw_ind) = 'Y'
        WHERE hcrcr.midas_pa_physician_advisor_id IN(
          204090, 204091, 291957, 291958, 291959, 291960
        )
        GROUP BY 1
    ) AS external_pa_referral ON fact_patient.patient_dw_id = external_pa_referral.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.chois_product_line ON upper(fact_patient.drg_code_hcfa) = upper(chois_product_line.drg_code)
     AND upper(chois_product_line.drg_code_group_ind) = 'M'
    LEFT OUTER JOIN (
      SELECT
          a.patient_dw_id,
          a.cert_end_date,
          a.cert_status_id,
          b.hcm_status_cause_name,
          row_number() OVER (PARTITION BY a.patient_dw_id ORDER BY a.encounter_payer_id, a.encounter_payer_cert_hist_id DESC, c.payer_status_id, a.encounter_payer_cert_id) AS row_num
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer_cert_hist AS a
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer_cert AS c ON a.encounter_payer_id = c.encounter_payer_id
           AND a.encounter_payer_cert_id = c.encounter_payer_cert_id
           AND upper(a.active_dw_ind) = 'Y'
           AND upper(c.active_dw_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_status_cause AS b ON a.cert_status_id = b.hcm_status_cause_id
           AND upper(b.active_dw_ind) = 'Y'
        WHERE upper(a.active_dw_ind) = 'Y'
         AND (a.patient_dw_id, coalesce(a.cert_end_date, DATE '1900-01-01')) IN(
          SELECT AS STRUCT
              cm_encounter_payer_cert_hist.patient_dw_id,
              max(coalesce(cm_encounter_payer_cert_hist_0.cert_end_date, DATE '1900-01-01'))
            FROM
              `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer_cert_hist AS cm_encounter_payer_cert_hist_1
            WHERE upper(cm_encounter_payer_cert_hist_2.active_dw_ind) = 'Y'
            GROUP BY 1
        )
    ) AS drv_max_cert_end ON drv_max_cert_end.patient_dw_id = fact_patient.patient_dw_id
     AND drv_max_cert_end.row_num = 1
    LEFT OUTER JOIN (
      SELECT
          a.patient_dw_id,
          min(a.cert_begin_date) AS min_cert_begin_date
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer_cert_hist AS a
        WHERE upper(a.active_dw_ind) = 'Y'
         AND a.cert_begin_date IS NOT NULL
        GROUP BY 1
    ) AS drv_min_cert_begin_date ON fact_patient.patient_dw_id = drv_min_cert_begin_date.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          delay_progression_detail.coid,
          delay_progression_detail.pat_acct_num,
          delay_progression_detail.case_type,
          CASE
            WHEN upper(delay_progression_detail.case_type) = 'EMERGENT' THEN 1
            WHEN upper(delay_progression_detail.case_type) = 'URGENT' THEN 2
            WHEN upper(delay_progression_detail.case_type) = 'PLANNED' THEN 3
          END AS case_type_order,
          row_number() OVER (PARTITION BY delay_progression_detail.coid, delay_progression_detail.pat_acct_num ORDER BY CASE
            WHEN upper(delay_progression_detail.case_type) = 'EMERGENT' THEN 1
            WHEN upper(delay_progression_detail.case_type) = 'URGENT' THEN 2
            WHEN upper(delay_progression_detail.case_type) = 'PLANNED' THEN 3
          END) AS priority_order
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_views.delay_progression_detail
        WHERE upper(delay_progression_detail.measure_alias) = 'COMPLETED CASES'
         AND upper(delay_progression_detail.role_alias_name) = 'ANY STAFF ROLE'
        GROUP BY 1, 2, 3, 4
    ) AS drv_surgical_case_type ON upper(drv_surgical_case_type.coid) = upper(fact_patient.coid)
     AND drv_surgical_case_type.pat_acct_num = fact_patient.pat_acct_num
     AND drv_surgical_case_type.priority_order = 1
  WHERE fact_facility.unit_num IN(
    '00038', '06659', '08625', '29002', '08621', '00001', '02903', '00025', '00145', '00631', '00447', '25073', '02902', '06020', '25070', '09391', '00031', '29026', '00584', '29027', '01269', '29025', '26620', '26627', '00037', '27200', '27100', '27101', '27490', '27491', '27495', '27400', '27401', '27450', '27300', '27150', '00323', '26330', '01489', '01605', '07922', '08298', '29001', '00187', '00372', '00009', '00460', '00162', '00417', '01533', '29004', '01601', '03198', '06218', '06437', '01512', '29010', '01515', '29009', '00196', '06240', '06676', '06678', '06679', '08778', '08224', '25067', '25960', '00322', '02857', '00136', '25901', '00015', '00058', '00073', '00444', '00950', '01505', '29006', '06251', '06264', '06742', '07126', '07923', '01574', '01578', '01588', '02531', '25164', '03132', '07458', '26109', '02348', '07850', '00034', '02560', '09731', '06194', '06212', '00450', '00637', '09720', '09721', '09722', '09723', '09724', '09725', '09726', '25159', '09727', '09729', '09728', '09730', '00068', '00079', '00138', '00954', '06030', '06042', '25016', '\r     00092', '00310', '01406', '29014', '00096', '00355', '00039', '00050', '06210', '29022', '00060', '00164', '01307', '29013', '00327', '01323', '29024', '26450', '00059', '00097', '00147', '00636', '00035', '00448', '02700', '02699', '25325', '00005', '00033', '00041', '02962', '08409', '08540', '09472', '00144', '25558', '00367', '01385', '29017', '01387', '29015', '26536', '00146', '00311', '00643', '00642', '01384', '29016', '26535', '26537', '00309', '01147', '01371', '29012', '01377', '29011', '00477', '01331', '29005', '02873', '00056', '00456', '29021', '01345', '29007', '01645', '29029', '00102', '00119', '00476', '01356', '29019', '01643', '29020', '26110', '26111', '01310', '29023', '02524', '02525', '01417', '29008', '00384', '08158', '08159', '08165', '00062', '00391', '00104', '01541', '29028', '02270', '08967', '03360', '08385', '00048', '01554', '29030', '07150', '00029', '00163', '29018', '06009', '00472', '00647', '01302', '01346', '29003', '08619', '00021', '02532', '09439'
  )
   AND denial_eom.new_denial_account_amt <> 0
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, upper(major_payor.major_payor_name), upper(coalesce(chois_product_line.chois_product_line_desc, 'DRG Not Assigned')), 15, upper(CASE
     fact_patient.financial_class_code
    WHEN 1 THEN 'MEDICARE - PPS'
    WHEN 2 THEN 'MEDICARE - DPU'
    WHEN 3 THEN 'MEDICAID'
    WHEN 4 THEN 'WORKERS COMP'
    WHEN 5 THEN 'COMMERCIAL'
    WHEN 6 THEN 'CHAMPUS'
    WHEN 7 THEN 'HMO'
    WHEN 8 THEN 'PPO'
    WHEN 9 THEN 'MANAGED CARE MEDICAID'
    WHEN 10 THEN 'FEDERAL'
    WHEN 11 THEN 'STATE NON MCAID LOCL GOV'
    WHEN 12 THEN 'MANAGED CARE MEDICARE'
    WHEN 13 THEN 'BLUE CROSS/COST'
    WHEN 14 THEN 'EXCHANGES'
    WHEN 15 THEN 'CHARITY'
    WHEN 99 THEN 'SELF PAY'
    WHEN 0 THEN 'UNASSIGNED'
    ELSE 'UNKNOWN PAYOR CLASS'
  END), 17, upper(bi_major_payor.major_payor_name), upper(ref_cc_root_cause.cc_root_cause_desc), 20, 21, 23, upper(CASE
     denial_eom.patient_financial_class_code
    WHEN 1 THEN 'MEDICARE - PPS'
    WHEN 2 THEN 'MEDICARE - DPU'
    WHEN 3 THEN 'MEDICAID'
    WHEN 4 THEN 'WORKERS COMP'
    WHEN 5 THEN 'COMMERCIAL'
    WHEN 6 THEN 'CHAMPUS'
    WHEN 7 THEN 'HMO'
    WHEN 8 THEN 'PPO'
    WHEN 9 THEN 'MANAGED CARE MEDICAID'
    WHEN 10 THEN 'FEDERAL'
    WHEN 11 THEN 'STATE NON MCAID LOCL GOV'
    WHEN 12 THEN 'MANAGED CARE MEDICARE'
    WHEN 13 THEN 'BLUE CROSS/COST'
    WHEN 14 THEN 'EXCHANGES'
    WHEN 15 THEN 'CHARITY'
    WHEN 99 THEN 'SELF PAY'
    WHEN 0 THEN 'UNASSIGNED'
    ELSE 'UNKNOWN PAYOR CLASS'
  END), upper(CASE
    WHEN denial_eom.denial_status_code IN(
      'WZ', 'XZ', 'YZ', 'W0', 'X0', 'Y0', 'WM', 'XM', 'YM'
    ) THEN 'All Coding Denial'
    WHEN denial_eom.denial_status_code IN(
      'WW', 'XW', 'YW', 'WR', 'XR', 'YR', 'W2', 'X2', 'Y2', 'W7', 'X7', 'Y7', 'W4', 'X4', 'Y4', 'W1', 'X1', 'Y1', 'W6', 'X6', 'Y6', 'W3', 'X3', 'Y3', 'W5', 'X5', 'Y5'
    ) THEN 'Information Requested/Other Technical Denial'
    WHEN denial_eom.denial_status_code IN(
      'WC', 'XC', 'YC', 'WT', 'XT', 'YT', 'WH', 'XH', 'YH', 'WI', 'XI', 'YI', 'WG', 'XG', 'YG', 'WJ', 'XJ', 'YJ', 'WU', 'XU', 'YU', 'WV', 'XV', 'YV', 'WK', 'XK', 'YK', 'WL', 'XL', 'YL'
    ) THEN 'Medical Necessity'
    WHEN denial_eom.denial_status_code IN(
      'W9', 'X9', 'Y9', 'W8', 'X8', 'Y8', 'WP', 'XP', 'YP', 'WQ', 'XQ', 'YQ', 'WO', 'XO', 'YO', 'WN', 'XN', 'YN', 'WA', 'XA', 'YA', 'WB', 'XB', 'YB', 'WS', 'XS', 'YS', 'WE', 'XE', 'YE', 'WF', 'XF', 'YF', 'WD', 'XD', 'YD'
    ) THEN 'No Auth/Notification'
    WHEN denial_eom.denial_status_code IN(
      '99', '98', '#EMPTY'
    ) THEN 'No Denial Code'
    WHEN denial_eom.denial_status_code IN(
      'WX', 'XX', 'YX', 'WY', 'XY', 'YY'
    ) THEN 'Timely Filing'
    WHEN denial_eom.denial_status_code IN(
      '8H', '8I', '8P'
    ) THEN 'Pending'
    ELSE 'No Denial Code'
  END), upper(CASE
    WHEN denial_eom.cc_root_cause_id IN(
      '520D      ', '520H      ', '520I      ', '521B      ', '522H      ', '523H      ', '524H      ', '525H      ', '526H      ', 'CDCDH     ', 'CDLAC     ', 'CDOVB     ', 'CDCD      ', 'CDLA      ', 'CDOV      '
    ) THEN 'Coding'
    WHEN denial_eom.cc_root_cause_id IN(
      '250B      ', '250H      ', '260B      ', '260C      ', '340C      ', '340D      ', '340I      ', '340B      ', '340H      ', '340R      ', '350D      ', '350H      ', '350I      ', '365D      ', '380D      ', '380R      ', '415C      ', 'IRMRO     ', 'IRPFB     ', 'IRSFR     ', 'IRMR      ', 'IRPF      ', 'IRSF      '
    ) THEN 'Info Req, Not Provided'
    WHEN denial_eom.cc_root_cause_id IN(
      '550P      ', '550R      ', '550V      ', 'IAREI     ', 'ICEFI     ', 'ICEIR     ', 'ICENR     ', 'ICPAP     ', 'IARE      ', 'ICEF      ', 'ICEI      ', 'ICEN      ', 'ICPA      '
    ) THEN 'I-Plan Update'
    WHEN denial_eom.cc_root_cause_id IN(
      '425D      ', '425P      ', '430C      ', '430D      ', '435I      ', '435D      ', '440C      ', '450C      ', '450D      ', '460C      ', '460D      ', '465C      ', '465D      ', '470D      ', '470I      ', '480B      ', '480C      ', '480D      ', '480I      ', '480R      ', '480V      ', '481I      ', '482C      ', 'MNLDC     ', 'MNMPI     ', 'MNMRC     ', 'MNNAI     ', 'MNSIB     ', 'MNSIC     ', 'MNSIR     ', 'MNTXI     ', 'MNLD      ', 'MNMP      ', 'MNMR      ', 'MNNA      ', 'MNSI      ', 'MNSI      ', 'MNSI      ', 'MNTX      '
    ) THEN 'Medical Necessity'
    WHEN denial_eom.cc_root_cause_id IN(
      '105D      ', '115D      ', '115R      ', '115V      ', '125C      ', '125I      ', '125R      ', '125V      ', '134C      ', '135C      ', '136C      ', '140I      ', '150I      ', '155C      ', '155R      ', '155V      ', '170I      ', 'IAAUC     ', 'IAIPR     ', 'IALIR     ', 'IAOPR     ', 'IAAU      ', 'IAIP      ', 'IALI      ', 'IAOP      '
    ) THEN 'Missing / Invalid Auth or Notification'
    WHEN denial_eom.cc_root_cause_id IN(
      '650A      ', '650B      ', '650C      ', '650I      ', '650U      ', '650V      ', '650P      ', 'IDAPI     ', 'IDHXI     ', 'IDIMI     ', 'IDMNI     ', 'IDNAI     ', 'IDAP      ', 'IDHX      ', 'IDIM      ', 'IDMN      ', 'IDNA      '
    ) THEN 'Inappropriate Denials'
    WHEN denial_eom.cc_root_cause_id IN(
      '530B      ', '530C      ', '530D      ', '530I      ', '530P      ', '530R      ', 'OTOTA     ', 'OTOTB     ', 'OTOTC     ', 'OTOTI     ', 'OTOTO     ', 'OTOTR     ', 'OTTMO     ', 'OTOT     ', 'OTTM      '
    ) THEN 'Other'
    WHEN denial_eom.cc_root_cause_id IN(
      '220A      ', '220B      ', '220C      ', '220H      ', '220V      ', '220R      ', '220U      ', '355P      ', '355I      ', 'TFCCB     ', 'TFOII     ', 'TFRBA     ', 'TFTFO     ', 'TFCC      ', 'TFOI      ', 'TFRB      ', 'TFTF      '
    ) THEN 'Timely Filing'
    WHEN denial_eom.cc_root_cause_id IN(
      'CTICC     ', 'CTPDI     ', 'CTSCC     ', 'CTIC      ', 'CTPD      ', 'CTSC      '
    ) THEN 'Timely Response - Clinicals'
    ELSE 'N/A'
  END), upper(CASE
    WHEN drv_denial_full_document.patient_dw_id IS NULL THEN 'N'
    WHEN drv_denial_full_document.patient_dw_id IS NOT NULL
     AND drv_denial_full_document.payor_financial_class_code IN(
      1, 2, 15, 99
    ) THEN 'N'
    ELSE 'Y'
  END), 28, 29, 30, 31, 32, 33, upper(CASE
    WHEN drv_partial_denial.patient_dw_id IS NULL THEN 'N'
    ELSE 'Y'
  END), upper(CASE
    WHEN upper(cm_encounter.iq_adm_rev_type_ip_ind) = 'Y'
     OR upper(cm_encounter.iq_adm_rev_type_obs_ind) = 'Y' THEN 'Y'
    ELSE 'N'
  END), upper(cm_encounter.iq_adm_rev_type_ip_mn_met_ind), upper(cm_encounter.iq_adm_rev_type_obs_mn_met_ind), upper(CASE
    WHEN pa_disposition_ip.patient_dw_id IS NULL THEN 'N'
    ELSE 'Y'
  END), upper(CASE
    WHEN pa_disposition_obs.patient_dw_id IS NULL THEN 'N'
    ELSE 'Y'
  END), upper(CASE
    WHEN external_pa_referral.patient_dw_id IS NULL THEN 'N'
    ELSE 'Y'
  END), 41, 42, 43, upper(drv_max_cert_end.hcm_status_cause_name), upper(cm_encounter.midas_acct_num), upper(format_date('%Y', fact_patient.discharge_date)), 47, upper(coalesce(fact_patient.drg_desc_hcfa, 'DRG Not Assigned')), upper(fact_patient.drg_code_hcfa), upper(fact_patient.admission_type_code), 51, 52, 53, 54, 55, upper(ref_cm_location.location_name)
;
