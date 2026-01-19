CREATE OR REPLACE VIEW edwpsc_views.`pv_stguacdetail`
AS SELECT
  `pv_stguacdetail`.practice,
  `pv_stguacdetail`.uacdetail_num,
  `pv_stguacdetail`.payment_num,
  `pv_stguacdetail`.trans_date,
  `pv_stguacdetail`.trans_type,
  `pv_stguacdetail`.trans_amt,
  `pv_stguacdetail`.trans_desc,
  `pv_stguacdetail`.db_acnt,
  `pv_stguacdetail`.cr_acnt,
  `pv_stguacdetail`.crt_userid,
  `pv_stguacdetail`.crt_datetime,
  `pv_stguacdetail`.uacdetailpk,
  `pv_stguacdetail`.org_uac_num,
  `pv_stguacdetail`.regionkey,
  `pv_stguacdetail`.inserteddtm,
  `pv_stguacdetail`.modifieddtm,
  `pv_stguacdetail`.dwlastupdatedatetime,
  `pv_stguacdetail`.sourcephysicaldeleteflag
  FROM
    edwpsc_base_views.`pv_stguacdetail`
;