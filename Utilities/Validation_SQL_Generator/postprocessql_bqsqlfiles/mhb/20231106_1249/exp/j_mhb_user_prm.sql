-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_user_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT mhb_user_wrk.rdc_sid
   FROM `hca-hin-dev-cur-clinical`.edwci_staging.mhb_user_wrk
   WHERE (coalesce(mhb_user_wrk.rdc_sid, 0),
          upper(coalesce(mhb_user_wrk.user_login_name, '')),
          coalesce(mhb_user_wrk.mhb_user_role_sid, 0),
          upper(coalesce(mhb_user_wrk.user_title_text, '')),
          upper(coalesce(mhb_user_wrk.user_first_name, '')),
          upper(coalesce(mhb_user_wrk.user_last_name, '')),
          upper(coalesce(mhb_user_wrk.user_full_name, '')),
          upper(coalesce(mhb_user_wrk.user_email_text, '')),
          upper(coalesce(mhb_user_wrk.external_phone_1_num_label_text, '')),
          upper(coalesce(mhb_user_wrk.external_phone_1_num, '')),
          upper(coalesce(mhb_user_wrk.external_phone_2_num_label_text, '')),
          upper(coalesce(mhb_user_wrk.external_phone_2_num, '')),
          upper(coalesce(mhb_user_wrk.external_phone_3_num_label_text, '')),
          upper(coalesce(mhb_user_wrk.external_phone_3_num, '')),
          upper(coalesce(mhb_user_wrk.external_phone_4_num_label_text, '')),
          upper(coalesce(mhb_user_wrk.external_phone_4_num, '')),
          upper(coalesce(mhb_user_wrk.sip_num, '')),
          upper(coalesce(mhb_user_wrk.internal_user_ind, '')),
          coalesce(mhb_user_wrk.active_dw_ind, format('%4d', 0)),
          coalesce(mhb_user_wrk.mhb_audit_trail_num, 0)) NOT IN
       (SELECT AS STRUCT -- coalesce(coid,'') ,
 -- coalesce(Company_Code,'') ,
 coalesce(mhb_user.rdc_sid, 0),
 upper(coalesce(mhb_user.user_login_name, '')),
 coalesce(mhb_user.mhb_user_role_sid, 0), -- coalesce(coid,'') ,
 -- coalesce(Company_Code,'') ,
 upper(coalesce(mhb_user.user_title_text, '')),
 upper(coalesce(mhb_user.user_first_name, '')),
 upper(coalesce(mhb_user.user_last_name, '')),
 upper(coalesce(mhb_user.user_full_name, '')),
 upper(coalesce(mhb_user.user_email_text, '')),
 upper(coalesce(mhb_user.external_phone_1_num_label_text, '')),
 upper(coalesce(mhb_user.external_phone_1_num, '')),
 upper(coalesce(mhb_user.external_phone_2_num_label_text, '')),
 upper(coalesce(mhb_user.external_phone_2_num, '')),
 upper(coalesce(mhb_user.external_phone_3_num_label_text, '')),
 upper(coalesce(mhb_user.external_phone_3_num, '')),
 upper(coalesce(mhb_user.external_phone_4_num_label_text, '')),
 upper(coalesce(mhb_user.external_phone_4_num, '')),
 upper(coalesce(mhb_user.sip_num, '')),
 upper(coalesce(mhb_user.internal_user_ind, '')),
 coalesce(mhb_user.active_dw_ind, format('%4d', 0)),
 coalesce(mhb_user.mhb_audit_trail_num, 0)
        FROM `hca-hin-dev-cur-clinical`.edwci.mhb_user) ) AS q