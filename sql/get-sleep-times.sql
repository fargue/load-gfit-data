select id, start_date, end_date,
	TO_CHAR((duration_ms || ' millisecond')::interval, 'HH24:MI:SS') AS sleep_time
from healthdata.healthdata.sleep_session ss 
order by start_date desc;

select ss.id, ss.start_date, ss.end_date,
	TO_CHAR((ss.duration_ms || ' millisecond')::interval, 'HH24:MI:SS') AS sleep_time,
	gfss.description stage,
	ssd.start_date,
	TO_CHAR((ssd.duration_ms || ' millisecond')::interval, 'HH24:MI:SS') AS sleep_stage_time
from healthdata.healthdata.sleep_session ss
join healthdata.healthdata.sleep_session_data ssd on ss.id = ssd.sleep_session_id 
join healthdata.healthdata.google_fit_sleep_stages gfss on ssd.sleep_stage_id = gfss.id 
order by ss.start_date desc, ssd.start_date desc;