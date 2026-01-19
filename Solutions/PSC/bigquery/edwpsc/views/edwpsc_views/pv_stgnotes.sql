CREATE OR REPLACE VIEW edwpsc_views.`pv_stgnotes`
AS SELECT
  `pv_stgnotes`.practice,
  `pv_stgnotes`.group_name,
  `pv_stgnotes`.type,
  `pv_stgnotes`.key_value,
  `pv_stgnotes`.subkey,
  `pv_stgnotes`.notes,
  `pv_stgnotes`.active,
  `pv_stgnotes`.last_upd_userid,
  `pv_stgnotes`.last_upd_datetime,
  `pv_stgnotes`.notespk,
  `pv_stgnotes`.notespk_txt,
  `pv_stgnotes`.patinfopk,
  `pv_stgnotes`.createdby,
  `pv_stgnotes`.createdon,
  `pv_stgnotes`.regionkey,
  `pv_stgnotes`.sysstarttime,
  `pv_stgnotes`.inserteddtm,
  `pv_stgnotes`.modifieddtm,
  `pv_stgnotes`.dwlastupdatedatetime,
  `pv_stgnotes`.sourcephysicaldeleteflag
  FROM
    edwpsc_base_views.`pv_stgnotes`
;