
---- Cancellations from Facs ans Clinicians. It gets the differences in days from the cancellation time to the other times.
----  Places each cancellation on the funnel accordingly. 
---  when (status = 4 and canceled_by_jr = user_id) or (canceled_status_reason in (5,10,11,12,13,14))  ---> Cancelled by clinician
---  where  nj.job_status_id in (15,16,17) ---> cancelled by facility
---  where job_status_id = 22 and ((scheduled_at_jr_tz is not null) or (status = 2)) ---> No call no show

with cancellation_1 as (

select * , 
nj.insert_date as insert_date_nj ,
nj.update_date as update_date_nj ,  --
nj.delete_date as delete_date_nj ,  
nj.closed_date as closed_date_nj ,   

(nj.insert_date at time zone 'utc') as insert_date_nj_tz ,
(nj.update_date at time zone 'utc') as update_date_nj_tz ,  --- This is the time of the cancellation by fac
(nj.delete_date at time zone 'utc') as delete_date_nj_tz ,
(nj.closed_date at time zone 'utc') as closed_date_nj_tz ,

nj.job_id as job_id_nj, jr.job_id as job_id_jr ,
jr.insert_date as insert_date_jr , 
(jr.insert_date at time zone 'utc') as insert_date_jr_tz ,
jr.delete_date as delete_date_nj  ,
jr.update_date as update_date_jr ,
scheduled_at as scheduled_at_jr_tz ,
jr.cancelled_at as cancelled_at_jr ,
jr.canceled_by as canceled_by_jr ,

row_number () over (partition by nj.job_id order by 
nj.job_id, scheduled_at desc nulls last, insert_date_jr_tz desc nulls last) as last_sched

from elite.elite_nursa_job nj
full join elite.elite_job_request jr on nj.job_id = jr.job_id
where nj.job_type = 'PER_DIEM'
		and nj.job_board_name is null
order by job_id_nj, scheduled_at

) , request_count as (

---- number of Request for a job:
select distinct job_id, count(job_id) as req_cnt
from elite.elite_job_request 
group by job_id
---- number of Views for a job:

), vws_count as (

select distinct job_id, count(job_id) as view_cnt  
from elite.elite_job_viewed 
group by job_id

), canceled_by_whom as (

SELECT job_id_nj, user_id as request_by_user, 
ROW_NUMBER ( )   
    OVER ( order by job_id_nj) as Cancellation_id ,
job_facility_id, job_status_id, 
job_id_jr ,
created_by, closed_by,  
approved_by, 
status, canceled_status_reason, "cancelationReason" ,
canceled_by_jr, scheduled_at_jr_tz , last_sched ,
rc.req_cnt ,
vc.view_cnt ,
insert_date_nj ,
insert_date_nj_tz ,
update_date_nj ,
update_date_nj_tz ,
insert_date_jr ,
insert_date_jr_tz ,
cancelled_at_jr ,

js.from_date_time_timestamp as shift_start_js_tz ,
js.to_date_time_timestamp as shift_end_js_tz ,

--- Just for testing:
case when (status = 4 and canceled_by_jr = user_id) or (canceled_status_reason in (5,10,11,12,13))
      then 'Cancelled by clinician' else 'Not cancelled by clinician' end as Clinician_canc ,
case when job_status_id in (15,16,17)
		then 'Cancelled by Facility'
	  when job_status_id = 22 then 'No call No show'
		else 'Not Cancelled by Facility' end as Facility_canc ,

---- Who cancelled the job:
case when (status = 4 and canceled_by_jr = user_id) or (canceled_status_reason in (5,10,11,12,13,14))
      then 1 else 0 end as Cancelled_by_clinician ,
case when job_status_id in (15,16,17)
		then 1 else 0 end as Cancelled_by_facility , --fac  
case when job_status_id = 22 and ((last_sched = 1 and scheduled_at_jr_tz is not null) or (status = 2) ) then 1
		else 0 end as No_call_No_show ,

--- Was the job scheduled:
case when (scheduled_at_jr_tz is not null) or (status = 2)
			then 1 else 0 end as is_scheduled --- each cancellation (job + user rquest could have been shdled or not)
from cancellation_1 c1
  left join request_count rc on c1.job_id_nj = rc.job_id
  left join vws_count vc on c1.job_id_nj = vc.job_id
  left join elite.elite_job_shifts js on c1.job_id_nj = js.job_id
order by job_id_nj

), times as (

select 
job_id_nj, request_by_user, job_facility_id, Cancelled_by_clinician, 
case when cbw.Cancelled_by_facility = 1 and 
cbw.Cancelled_by_clinician = 0 then 1 else 0 end as Cancelled_by_facility, 
No_call_No_show, 
is_scheduled , view_cnt, req_cnt , 
canceled_by_jr, --- Cancellation_id,
job_status_id, status as status_jr, canceled_status_reason as canceled_status_reason_jr, 
"cancelationReason" as cancelation_Reason_jr,
EXTRACT(EPOCH from (shift_end_js_tz - shift_start_js_tz))/3600 as shift_length_hrs ,
shift_start_js_tz,
-- job_id_jr ,
created_by as created_by_nj, update_date_nj_tz, closed_by as closed_by_nj , 
insert_date_jr_tz as clinician_request_date ,
approved_by as approved_by_jr ,
scheduled_at_jr_tz ,
cancelled_at_jr ,

--- This times are going to be in hours:
case when Cancelled_by_clinician = 1 then  ---- cancelled - posted
EXTRACT(EPOCH from (cancelled_at_jr - insert_date_nj_tz))/3600 else null end as Canc_post_clin_diff_d ,
case when (Cancelled_by_facility = 1) and (Cancelled_by_clinician = 0) then
EXTRACT(EPOCH from (update_date_nj_tz - insert_date_nj_tz))/3600 else null end as Canc_post_fac_diff_d ,

case when Cancelled_by_clinician = 1 then    ---- cancelled - requested
EXTRACT(EPOCH from (cancelled_at_jr - insert_date_jr_tz))/3600 else null end as canc_rqst_clin_diff_d ,
case when (Cancelled_by_facility = 1) and (Cancelled_by_clinician = 0) then
EXTRACT(EPOCH from (update_date_nj_tz - insert_date_jr_tz))/3600 else null end as canc_rqst_fac_diff_d ,

case when Cancelled_by_clinician = 1 then   ----  cancelled - scheduled
EXTRACT(EPOCH from (cancelled_at_jr - scheduled_at_jr_tz))/3600 else null end as Canc_schd_clin_diff_d ,
case when (Cancelled_by_facility = 1) and (Cancelled_by_clinician = 0) then
EXTRACT(EPOCH from (update_date_nj_tz - scheduled_at_jr_tz))/3600 else null end as Canc_schd_fac_diff_d ,

case when Cancelled_by_clinician = 1 then   ----  cancelled - shift_start
EXTRACT(EPOCH from (shift_start_js_tz - cancelled_at_jr))/3600 else null end as Shift_Canc_clin_diff_d ,
case when (Cancelled_by_facility = 1) and (Cancelled_by_clinician = 0) then
EXTRACT(EPOCH from (shift_start_js_tz - update_date_nj_tz))/3600 else null end as Shift_Canc_fac_diff_d

from canceled_by_whom cbw
where (Cancelled_by_clinician = 1     ---- Important filter!
       or Cancelled_by_facility = 1 or
       No_call_No_show = 1)

) ,

classification as (
 
select *,
 
 case when is_scheduled = 0 then 0
	 when Canc_schd_clin_diff_d < 0 or Canc_schd_fac_diff_d < 0 then NULL
	 when Canc_schd_clin_diff_d >= 0 or Canc_schd_fac_diff_d >= 0 then 1
	    else NULL end as Canc_after_scheduled ,

 case
	when (Shift_Canc_clin_diff_d < 0)  and (Cancelled_by_clinician = 1)
	then 'On_the_job'
	  when Shift_Canc_clin_diff_d <= 0.5 and Shift_Canc_clin_diff_d >= 0 and Cancelled_by_clinician = 1
      then 	'Short_notice'
        when Shift_Canc_clin_diff_d > 0.5 and Cancelled_by_clinician = 1
	    then 'Advanced_noticed'
	      else NULL
end as Shift_Canc_by_clin ,

case
	when Shift_Canc_fac_diff_d < 0 and Cancelled_by_facility = 1 
	then 'On_the_job'
	  when Shift_Canc_fac_diff_d <= 0.5 and Shift_Canc_fac_diff_d >= 0 and Cancelled_by_facility = 1
      then 'Short_notice'
        when Shift_Canc_fac_diff_d > 0.5 and Cancelled_by_facility = 1
	    then 'Advanced_noticed'
	      else NULL
end as Shift_Canc_by_fac ,

case 
	when canc_rqst_clin_diff_d < 0 and Cancelled_by_clinician = 1
	then NULL
	  when canc_rqst_clin_diff_d >= 0 and canc_rqst_clin_diff_d <= 0.5 and Cancelled_by_clinician = 1
      then 'Fast_cancellation'
        when canc_rqst_clin_diff_d > 0.5 and canc_rqst_clin_diff_d <= 1 and Cancelled_by_clinician = 1
	    then '12_to_24_hrs'
	      when canc_rqst_clin_diff_d > 1 and Cancelled_by_clinician = 1
	      then 'MoreThan_24_hrs'
	      else NULL
end as Cancel_Request_Clinic ,  

case 
	when canc_rqst_fac_diff_d < 0 and Cancelled_by_facility = 1
	then NULL 
	  when canc_rqst_fac_diff_d >= 0 and canc_rqst_fac_diff_d <= 0.5 and Cancelled_by_facility = 1
      then 'Before_12_hrs'
        when canc_rqst_fac_diff_d > 0.5 and canc_rqst_fac_diff_d <= 1 and Cancelled_by_facility = 1
	    then '12_to_24_hrs'
	      when canc_rqst_fac_diff_d > 1
	      then 'MoreThan_24_hrs'
	      else NULL
end as Cancel_Request_Fac ,

case 
	when Canc_post_fac_diff_d < 0 and Cancelled_by_facility = 1
	then NULL 
	  when Canc_post_fac_diff_d >= 0 and Canc_post_fac_diff_d <= 0.5 and Cancelled_by_facility = 1
      then 'Fast cancellation'
        when Canc_post_fac_diff_d > 0.5 and Canc_post_fac_diff_d <= 1 and Cancelled_by_facility = 1
	    then '12_to_24_hrs'
	      when (Canc_post_fac_diff_d > 1 and Canc_post_fac_diff_d <= 3) and Cancelled_by_facility = 1
	      then '1_to_3_days'
	        when ( Canc_post_fac_diff_d > 3 ) and Cancelled_by_facility = 1
	        then 'MoreThan_3_days'
	         when Cancelled_by_facility = 0
	         then 'Not_cancel_by_Fac'
	           else NULL
end as Cancel_Post_Fac ,

case when (view_cnt = 0 or view_cnt is null) and req_cnt > 0 then NULL
		when (view_cnt = 0 or view_cnt is null) and (req_cnt = 0 or req_cnt is null) then 'Not viewed'
		when view_cnt > 0 and (req_cnt = 0 or req_cnt is null) then 'Viewed - Not requested'
		when view_cnt > 0 and req_cnt > 0 and (is_scheduled = 0 or is_scheduled is null) then 'Requested - Not scheduled'
		when No_call_No_show = 1 and cancelled_by_clinician = 0 then 'No call no show'
		when is_scheduled = 1 then 'Scheduled - Not completed'
		else NULL
		end as Stage,

case when (view_cnt = 0 or view_cnt is null) then 0 else 1 end as canc_after_view,
case when (request_by_user is null and clinician_request_date is null and 
			scheduled_at_jr_tz is null and 
			 (req_cnt = 0 or req_cnt is null)) then 0 else 1 end as canc_after_request

from times

)

select job_id_nj as job_id, 
job_facility_id, 
shift_length_hrs,
shift_start_js_tz as shift_start,
request_by_user, 
clinician_request_date, 
scheduled_at_jr_tz as scheduled_at, 
case when ((view_cnt is null) or (view_cnt = 0)) then 0 else 1 end as viewed ,
case when ((req_cnt is null) or (req_cnt = 0)) then 0 else 1 end as requested ,
is_scheduled as scheduled,

cancelled_by_clinician, 
cancelation_reason_jr as cancelation_reason,
--status_jr as status_jobrequest,
cancelled_at_jr as cancelled_by_clin_at, 
canc_post_clin_diff_d as post_canc_diff_clin,
canc_rqst_clin_diff_d as req_canc_diff_clin, 
canc_schd_clin_diff_d as sched_canc_diff_clin,
shift_canc_clin_diff_d as shift_canc_diff_clin,
--cancel_request_clinic,
--shift_canc_by_clin,

cancelled_by_facility,
update_date_nj_tz as cancelled_by_fac_at,
canc_post_fac_diff_d as post_canc_diff_fac,
canc_rqst_fac_diff_d as req_canc_diff_fac,
canc_schd_fac_diff_d as sched_canc_diff_fac,
shift_canc_fac_diff_d as shift_canc_diff_fac,
--cancel_post_fac, 
--cancel_request_fac, 
--shift_canc_by_fac,

no_call_no_show,

canc_after_view,  
canc_after_request,  
canc_after_scheduled,
Stage

from classification

