CREATE OR REPLACE VIEW edwpsc_views.`pv_stgerafilename`
AS SELECT
  `pv_stgerafilename`.fileid,
  `pv_stgerafilename`.filename,
  `pv_stgerafilename`.fullfilename,
  `pv_stgerafilename`.filedate,
  `pv_stgerafilename`.isa06,
  `pv_stgerafilename`.isa07,
  `pv_stgerafilename`.isa08,
  `pv_stgerafilename`.isa09,
  `pv_stgerafilename`.isa10,
  `pv_stgerafilename`.isa11,
  `pv_stgerafilename`.isa12,
  `pv_stgerafilename`.isa13,
  `pv_stgerafilename`.isa14,
  `pv_stgerafilename`.isa15,
  `pv_stgerafilename`.isa16,
  `pv_stgerafilename`.isasegment,
  `pv_stgerafilename`.inserteddtm,
  `pv_stgerafilename`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_stgerafilename`
;