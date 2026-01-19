CREATE OR REPLACE VIEW edwpsc_base_views.`pv_junciplancoid`
AS SELECT
  `pv_junciplancoid`.junciplancoidkey,
  `pv_junciplancoid`.iplankey,
  `pv_junciplancoid`.coid,
  `pv_junciplancoid`.insertedby,
  `pv_junciplancoid`.inserteddtm,
  `pv_junciplancoid`.modifiedby,
  `pv_junciplancoid`.modifieddtm,
  `pv_junciplancoid`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_junciplancoid`
;