CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgstactiongrp`
AS SELECT
  `artiva_stgstactiongrp`.stactgid,
  `artiva_stgstactiongrp`.stactgmsec,
  `artiva_stgstactiongrp`.stactgsdsc,
  `artiva_stgstactiongrp`.stactgser,
  `artiva_stgstactiongrp`.stactgtbl,
  `artiva_stgstactiongrp`.stactgtyp
  FROM
    edwpsc_base_views.`artiva_stgstactiongrp`
;