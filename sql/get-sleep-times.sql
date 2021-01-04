select id, start_date, end_date, ss.google_fit_modified_date,
	TO_CHAR((duration_ms || ' millisecond')::interval, 'HH24:MI:SS') AS sleep_time,
	active
from healthdata.healthdata.sleep_session ss 
where active = TRUE
order by start_date desc;

select ss.end_date, ss.duration_ms::float/1000/60/60 AS sleep_hr
from healthdata.healthdata.sleep_session ss 
where ss.active = TRUE
and ss.end_date > '2020-12-01'
order by ss.end_date asc;


update healthdata.healthdata.sleep_session ss set active = false where start_date in (
'2020-12-18 22:40:00','2020-12-14 22:07:00','2020-12-13 22:28:00','2020-12-09 21:29:00',
'2020-12-06 23:06:00','2020-12-05 01:02:00','2020-12-03 22:16:00','2020-11-30 21:57:00');
commit;
select ss.id, ss.start_date, ss.end_date,
	TO_CHAR((ss.duration_ms || ' millisecond')::interval, 'HH24:MI:SS') AS sleep_time,
	gfss.description stage,
	ssd.start_date,
	TO_CHAR((ssd.duration_ms || ' millisecond')::interval, 'HH24:MI:SS') AS sleep_stage_time
from healthdata.healthdata.sleep_session ss
join healthdata.healthdata.sleep_session_data ssd on ss.id = ssd.sleep_session_id 
join healthdata.healthdata.google_fit_sleep_stages gfss on ssd.sleep_stage_id = gfss.id 
order by ss.start_date desc, ssd.start_date desc;