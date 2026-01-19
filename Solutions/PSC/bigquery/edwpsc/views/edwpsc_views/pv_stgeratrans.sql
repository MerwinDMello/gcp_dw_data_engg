CREATE OR REPLACE VIEW edwpsc_views.`pv_stgeratrans`
AS SELECT
  `pv_stgeratrans`.practice,
  `pv_stgeratrans`.era_date,
  `pv_stgeratrans`.era_num,
  `pv_stgeratrans`.receiver_name,
  `pv_stgeratrans`.receiver_id,
  `pv_stgeratrans`.sender_name,
  `pv_stgeratrans`.sender_id,
  `pv_stgeratrans`.isa_ctrl_num,
  `pv_stgeratrans`.environment,
  `pv_stgeratrans`.check_nums,
  `pv_stgeratrans`.era_post_date,
  `pv_stgeratrans`.parsed_report,
  `pv_stgeratrans`.posted_report,
  `pv_stgeratrans`.exception_report,
  `pv_stgeratrans`.last_upd_userid,
  `pv_stgeratrans`.last_upd_datetime,
  `pv_stgeratrans`.eratranspk,
  `pv_stgeratrans`.docrootid,
  `pv_stgeratrans`.docpath,
  `pv_stgeratrans`.erafilename,
  `pv_stgeratrans`.regionkey,
  `pv_stgeratrans`.dwlastupdatedatetime,
  `pv_stgeratrans`.sourcephysicaldeleteflag
  FROM
    edwpsc_base_views.`pv_stgeratrans`
;