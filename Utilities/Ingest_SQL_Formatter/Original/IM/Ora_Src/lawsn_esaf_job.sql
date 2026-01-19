SELECT
	cmpy,
	job_cd,
	to_char(CAST(desc_updt_dt AS DATE),'YYYY-MM-DD') AS desc_updt_dt,
	job_actv_flg,
	job_cd_desc,
	job_cl,
	'v_currtimestamp' AS dw_last_update_date_time
FROM
	(
	SELECT
		t1.cmpy,
		t1.job_cd,
		t1.desc_updt_dt,
		t1.job_actv_flg,
		t1.job_cd_desc,
		t1.job_cl
	FROM
		orafs.vw_esaf_job t1
UNION
	SELECT
		t2.cmpy,
		t2.job_cd,
		t2.desc_updt_dt,
		t2.job_actv_flg,
		t2.job_cd_desc,
		t2.job_cl
	FROM
		oracorp.vw_esaf_job t2)