SET mapred.reduce.tasks=50;
SET hive.exec.reducers.bytes.per.reducer=4000000000;
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;
use hdp_oc_team;
drop table if exists surv_shop;
create table surv_shop ( shopper_id string, segment string, prem_svc int, tenure_mon int, isDoNotCall int
                                , lclecd string, region string, sub_region string, countryCode string, r float );
; with reseller as (
-- exclude resellers and customers with upcoming manual expirations
select  b.shopper_id
        , max(case when b.auto_renewal_flag = 0 and expiration_date
            between from_unixtime(unix_timestamp()) and date_add(from_unixtime(unix_timestamp()),30) then 1 else 0 end) as manual_renew_30
from    dm_ecommerce.fact_billing b
join    bigreporting.dim_product_snap dp
    on  b.pf_id = dp.pf_id
where   b.billing_status_id = 1
group by shopper_id
having manual_renew_30 = 1
), survey_sent as (
-- exclude customers who have recieved a survey in the last 6 months
select    distinct shopper_id
from    dbmarketing.unified_contact_email
where    sent_date >= date_sub(FROM_UNIXTIME(UNIX_TIMESTAMP()),183)
        and communication_id = '1000gm2dx7ph'  -- This needs to be populated with the NPS communication identifier
)
insert into table surv_shop
-- get basic shopper information and 10k sample
select  cdl.shopper_id
        , case when g.name in ('Domainer','Established','Moonlighter','Nascent','Nonprofit','Personal','WebPro','eCommerce') then g.name
                when g.name = 'Growth Business Size Unknown' then 'GrowthBiz'
                when g.name = 'Small & Hungry' then 'SmallHungry'
                when g.name = 'Up & Running' then 'UpRunning'
                else 'NoSegment' end segment
        , case when cdl.c_xxx_crmportfoliotypeid in (1,2,4) then 1 else 0 end
        , cast(ceil(datediff(to_date(from_unixtime(unix_timestamp())),to_date(cdl.t_xxx_firstorderdate)) / (365.25/12)) as int)
        , case when cdl.b_xxx_isdonotcall = 1 or cdl.x_xxx_postalstatecode = 'IN' then 1 else 0 end
        , s.lclecd
        , c.report_region_1_name as region
        , c.report_region_2_name as sub_region
        , upper(cdl.c_xxx_countrycode) countryCode
        , cdl.x_xxx_daily_rand
from    dbmarketing.cdl cdl
join      cust_customertracking.segment_snap g
    on  cdl.c_xxx_segmentcode = g.segmentCode
        and cdl.c_xxx_segmentsourcecode = 1
join    arm_views.vfshopacct s
    on  cdl.shopper_id = s.shopacctID
         and s.lclecd like 'en-%'
left join dm_reference.dim_geography c
    on  lower(cdl.c_xxx_countrycode) = lower(c.country_code)
left join reseller r
    on   cdl.shopper_id = r.shopper_id
left join survey_sent b
    on  cdl.shopper_id = b.shopper_id
where    cdl.c_xxx_privatelabelid = 1
        and cdl.t_xxx_firstorderdate is not null
        and datediff(FROM_UNIXTIME(UNIX_TIMESTAMP()),cdl.t_xxx_firstorderdate) > 30
        and cdl.b_xxx_isattrition_computed = 0
        and cdl.b_xxx_mktg_email_optin = 1
        and r.shopper_id is null
        and b.shopper_id is null
        and cdl.c_xxx_countrycode = 'US'
order by cdl.x_xxx_daily_rand
limit 10000
;
use hdp_oc_team;
drop table if exists surv_ord;
create table surv_ord ( shopper_id string, ltv decimal(11,2), last_chan string );
; with last_ord as (
select  s.shopper_id
        , o.order_isc_channel_name as channel
        , o.order_id
        , o.row_id
        , row_number() over ( partition by o.shopper_id order by o.order_ts desc, o.order_id desc ) rw
        , o.gcr_amt as gcr
        , o.item_tracking_code
from    surv_shop s
join     dp_enterprise.uds_order o
    on  s.shopper_id = o.shopper_id
)
insert into table surv_ord
select  shopper_id
           , sum(gcr)
           , max(case when rw = 1 and row_id = 0 then channel end)
from    last_ord
group by shopper_id
;
use hdp_oc_team;
drop table if exists surv_engage;
create table surv_engage ( shopper_id string, eng_recent int, eng_tot int );
; with engage as (
select  s.shopper_id
           , min(datediff(from_unixtime(unix_timestamp()),v.visittrackingdate)) recent
           , count(distinct visittracking_id) tot
from    surv_shop s
join     bisandbox.visitshopperid v
    on  s.shopper_id = v.shopperid
         and v.visittrackingdate >= date_sub(to_date(from_unixtime(unix_timestamp())),90)
group by s.shopper_id
union all
select  s.shopper_id
           , min(datediff(from_unixtime(unix_timestamp()),i.dateStamp)) recent
           , count(distinct crm_ucidkey) tot
from    surv_shop s
join     callcenterreporting.rptfactc3inboundcall_snap i
    on  s.shopper_id = i.shopperID
          and i.dateStamp >= date_sub(to_date(from_unixtime(unix_timestamp())),90)
group by s.shopper_id
)
insert into table surv_engage
select  shopper_id
           , min(recent)
           , sum(tot)
from engage
group by shopper_id
;
use hdp_oc_team;
drop table if exists surv_bill;
create table surv_bill ( shopper_id string, pnl_lines int, resc_nondom int, resc_dom int
                                , dmn_addon int, gf int, hosting int, gdob int
                                , off_tools int, ssl int, wsb int, cnp int, wp_manage int, grid int, wp int
                                , shared int, o365_on_ess int, o365_bus_prem int, o365_bus int, email int
                                , cal int, storage int, o365_resc int, ssl_resc int, min_expire date )
;
insert into table surv_bill
select  s.shopper_id
        , count(distinct dp.pnl_line) pnl_lines
        , count(distinct case when dp.pnl_group <> 'Domains' then b.resource_id end) resc_nondom
        , count(distinct case when dp.pnl_group = 'Domains' then b.resource_id end) resc_dom
        , max(case when dp.pnl_category = 'Domain Add Ons' then 1 else 0 end) dmn_addon
        , max(case when dp.pnl_line = 'Get Found' then 1 else 0 end) gf
        , max(case when dp.pnl_line in ('Grid','Shared Hosting','Wordpress','CnP Hosting') then 1 else 0 end) hosting
        , max(case when dp.pnl_line = 'Online Bookkeeping' then 1 else 0 end) gdob
        , max(case when dp.pnl_line in ('Email','MS Office 365','Online Calendar') then 1 else 0 end) off_tools
        , max(case when dp.pnl_line = 'SSL' then 1 else 0 end) ssl
        , max(case when dp.pnl_line = 'Website Builder' then 1 else 0 end) wsb
        , max(case when dp.pnl_line = 'CnP Hosting' then 1 else 0 end) cnp
        , max(case when dp.pnl_line = 'WordPress Managed Plans' then 1 else 0 end) wp_manage
        , max(case when dp.pnl_line = 'Grid' then 1 else 0 end) grid
        , max(case when dp.pnl_line = 'Wordpress' then 1 else 0 end) wp
        , max(case when dp.pnl_line = 'Shared Hosting' then 1 else 0 end) shared
        , max(case when dp.pnl_line = 'MS Office 365' and dp.pnl_subline = 'Online Essentials' then 1 else 0 end) o365_on_ess
        , max(case when dp.pnl_line = 'MS Office 365' and dp.pnl_subline = 'Business Premium' then 1 else 0 end) o365_bus_prem
        , max(case when dp.pnl_line = 'MS Office 365' and dp.pnl_subline = 'Business' then 1 else 0 end) o365_bus
        , max(case when dp.pnl_line = 'Email' then 1 else 0 end) email
        , max(case when dp.pnl_line = 'Online Calendar' then 1 else 0 end) cal
        , max(case when dp.pnl_line = 'Online Storage' then 1 else 0 end) storage
        , count(distinct case when dp.pnl_line = 'MS Office 365' then resource_id end) o365_resc
        , count(distinct case when dp.pnl_line = 'SSL' then resource_id end) ssl_resc
        , min(b.expiration_date) min_expire
from    surv_shop s
join    dm_ecommerce.fact_billing b
    on  s.shopper_id = b.shopper_id
join    bigreporting.dim_product_snap dp
    on  b.pf_id = dp.pf_id
where   b.billing_status_id = 1
        and b.expiration_date > to_date(from_unixtime(unix_timestamp()))
group by s.shopper_id
;
use hdp_oc_team;
drop table if exists surv_dom;
create table surv_dom ( shopper_id string, own_domain int, domains int, domains_lt int );
insert into table surv_dom
select  s.shopper_id
           , max(case when ds.isactiveflag = 1 then 1 else 0 end) own_domain
           , count(distinct case when ds.isactiveflag = 1 then d.id end) domains
           , count(distinct d.id) domains_lt
from    surv_shop s
join    domains.domaininfo_snap d
    on  s.shopper_id = d.shopper_id
join    domains.domaininfo_status_snap ds
    on  d.status = ds.domaininfo_statusid
group by s.shopper_id
;
use hdp_oc_team;
drop table if exists surv_shopInfo;
create table surv_shopInfo ( shopper_id string, first_name string, last_name string, phone1 string, gender string
                                        , city string, state string, zip string );
insert into table surv_shopInfo
select  s.shopper_id
            , f.first_name
            , f.last_name
            , f.phone1
            , f.gender
            , f.city
            , f.state
            , f.zip
from    surv_shop s
join      fortknox.fortknox_shopper_snap f
    on  s.shopper_id = f.shopper_id
;
use hdp_oc_team;
drop table if exists surv_survey;
create table surv_survey ( shopper_id string, industry string, rev string );
; with surv as (
select  s.shopper_id
            , ta.questionID
            , max(ta.answerID) ans
from    cust_survey.csf_surveytakenanswers_snap ta
join    cust_survey.csf_surveytaken_snap t
    on  t.csfSurveyTakenId = ta.csfSurveyTakenId
join    cust_survey.csf_surveysource_snap ss
    on  ss.csfsurveytakenid = t.csfsurveytakenid
join    surv_shop s
    on  s.shopper_id = ss.shopperid
where   ta.questionID in (3170,3173)
group by s.shopper_id, ta.questionID
)
insert into table surv_survey
select  s.shopper_id
            , max(case when s.questionID = 3170 then a.answertext end) ind
            , max(case when s.questionID = 3173 then a.answertext end) rev
from    surv s
join      cust_survey.csf_answer_snap a
    on   s.ans = a.answerID
group by s.shopper_id
;
drop table if exists surv_pms;
create table surv_pms ( shopper_id string, act_wsb int, pub_wsb int, act_ssl int );
insert into table surv_pms
select  s.shopper_id
           , max(case when i.metricId IN (43,474) then 1 else 0 end) as act_wsb
           , max(case when i.metricId IN (45,384) then 1 else 0 end) as pub_wsb
           , max(case when i.metricId = 317 then 1 else 0 end) as act_ssl
from    productmetricscoring.score_metricinstance_snap i
join     productmetricscoring.score_shopperresource_snap r
    on  i.shopperresourceid = r.shopperresourceid
join     surv_shop s
    on  r.shopper_id = s.shopper_id
where   i.metricID in (474, 384, 317)
group by s.shopper_id
;
use hdp_oc_team;
drop table if exists ShopNPSUpload;
create table ShopNPSUpload ( survey_medium string, shopper_id string, personID string, pf_type string, prods string, wsb_version string, own_domain string, own_dmnaddon string
                                    , own_gf string, own_hosting string, own_gdob string, own_offtools string, own_ssl string
                                    , own_wsb string, own_cnp string, own_wpmanage string, own_grid string, own_wp string
                                    , own_shared string, own_o365_on_ess string, own_o365_bus_prem string, own_o365_bus string
                                    , own_email string, own_cal string, own_storage string, o365_accts string, curr_dom string
                                    , lt_dom string, ssl_resc string, rev_p_mon string, rev_p_yr string, tenure string, sub_region string
                                    , region string, first_name string, last_name string, phone1 string, gender string, city string
                                    , state string, zip string, email string, eng_recent string, eng_tot string, surveyID string
                                    , follow1 string, contact string, contact_mgr string, status string, launch_type string
                                    , wsb_active string, wsb_pub string, ssl_active string, ltv string, last_chan string
                                    , lang string, segment string, prem_svc string, tenure_mon int, dnc string, rev string
                                    , industry string, escal1 string )
stored as textfile
;
insert into table ShopNPSUpload
select  'paper' as survey_medium
			, a.shopper_id
            , a.shopper_id as personID
           , case when b.resc_dom > 0 and b.resc_nondom > 0 then 'Domain+Product'
                    when b.resc_dom > 0 and b.resc_nondom = 0 then 'Domain Only'
                    when b.resc_dom = 0 and b.resc_nondom > 0 then 'Product Only'
                    when coalesce(b.resc_dom,0) = 0 and coalesce(b.resc_nondom,0) = 0 then 'No Products'
                    else '???' end as pf_type
            , case when coalesce(b.resc_dom,0)+coalesce(b.resc_nondom,0) = 0 then '0 Products'
                        when coalesce(b.resc_dom,0)+coalesce(b.resc_nondom,0) = 1 then '1 Product'
                        when coalesce(b.resc_dom,0)+coalesce(b.resc_nondom,0) = 2 then '2 Products'
                        when coalesce(b.resc_dom,0)+coalesce(b.resc_nondom,0) = 3 then '3 Products'
                        when coalesce(b.resc_dom,0)+coalesce(b.resc_nondom,0) < 6 then '4-5 Products'
                        when coalesce(b.resc_dom,0)+coalesce(b.resc_nondom,0) < 11 then '6-10 Products'
                        when coalesce(b.resc_dom,0)+coalesce(b.resc_nondom,0) < 21 then '11-20 Products'
                        when coalesce(b.resc_dom,0)+coalesce(b.resc_nondom,0) >= 21 then '21+ Products'
                        else '???' end as prods
			, '' as wsb_version
            , case when c.own_domain is not null then 'Yes' else 'No' end own_dom
            , case when b.dmn_addon = 1 then 'Yes' else 'No' end own_dmnaddon
            , case when b.gf = 1 then 'Yes' else 'No' end own_gf
            , case when b.hosting = 1 then 'Yes' else 'No' end own_hosting
            , case when b.gdob = 1 then 'Yes' else 'No' end own_gdob
            , case when b.off_tools = 1 then 'Yes' else 'No' end as own_offtools
            , case when b.ssl = 1 then 'Yes' else 'No' end as own_ssl
            , case when b.wsb = 1 then 'Yes' else 'No' end as own_wsb
            , case when b.cnp = 1 then 'Yes' else 'No' end as own_cnp
            , case when b.wp_manage = 1 then 'Yes' else 'No' end as own_wpmanage
            , case when b.grid = 1 then 'Yes' else 'No' end as own_grid
            , case when b.wp = 1 then 'Yes' else 'No' end as own_wp
            , case when b.shared = 1 then 'Yes' else 'No' end as own_shared
            , case when b.o365_on_ess = 1 then 'Yes' else 'No' end as own_o365_on_ess
            , case when b.o365_bus_prem = 1 then 'Yes' else 'No' end as own_o365_bus_prem
            , case when b.o365_bus = 1 then 'Yes' else 'No' end as own_o365_bus
            , case when b.email = 1 then 'Yes' else 'No' end as own_email
            , case when b.cal = 1 then 'Yes' else 'No' end as own_cal
            , case when b.storage = 1 then 'Yes' else 'No' end as own_storage
            , case when coalesce(b.o365_resc,0) = 0 then '0 Accounts'
                        when coalesce(b.o365_resc,0) < 3 then '1-2 Accounts'
                        when coalesce(b.o365_resc,0) < 6 then '3-5 Accounts'
                        when coalesce(b.o365_resc,0) < 11 then '6-10 Accounts'
                        when coalesce(b.o365_resc,0) < 21 then '11-20 Accounts'
                        when coalesce(b.o365_resc,0) < 51 then '21-50 Accounts'
                        when coalesce(b.o365_resc,0) < 101 then '51-100 Accounts'
                        else '100+ Accounts' end as o365_accts
            , case when coalesce(c.domains,0) = 0 then '0 Domains'
                        when coalesce(c.domains,0) = 1 then '1 Domain'
                        when coalesce(c.domains,0) = 2 then '2 Domains'
                        when coalesce(c.domains,0) < 6 then '3-5 Domains'
                        when coalesce(c.domains,0) < 11 then '6-10 Domains'
                        when coalesce(c.domains,0) < 21 then '11-20 Domains'
                        when coalesce(c.domains,0) < 51 then '21-50 Domains'
                        when coalesce(c.domains,0) < 101 then '51-100 Domains'
                        else '100+ Domains' end as curr_dom
            , case when coalesce(c.domains_lt,0) = 0 then '0 Domains'
                        when coalesce(c.domains_lt,0) = 1 then '1 Domain'
                        when coalesce(c.domains_lt,0) = 2 then '2 Domains'
                        when coalesce(c.domains_lt,0) < 6 then '3-5 Domains'
                        when coalesce(c.domains_lt,0) < 11 then '6-10 Domains'
                        when coalesce(c.domains_lt,0) < 21 then '11-20 Domains'
                        when coalesce(c.domains_lt,0) < 51 then '21-50 Domains'
                        when coalesce(c.domains_lt,0) < 101 then '51-100 Domains'
                        else '100+ Domains' end as lt_dom
            , case when coalesce(b.ssl_resc,0) = 0 then '0 Resources'
                        when coalesce(b.ssl_resc,0) = 1 then '1 Resource'
                        when coalesce(b.ssl_resc,0) = 2 then '2 Resources'
                        when coalesce(b.ssl_resc,0) < 6 then '3-5 Resources'
                        when coalesce(b.ssl_resc,0) < 11 then '6-10 Resources'
                        else '11+ Resources' end as ssl_resc
            , case when coalesce(h.ltv,0) / a.tenure_mon <= 5 then '$1-5'
                        when coalesce(h.ltv,0) / a.tenure_mon <= 10 then '$5-10'
                        when coalesce(h.ltv,0) / a.tenure_mon <= 15 then '$10-15'
                        when coalesce(h.ltv,0) / a.tenure_mon <= 30 then '$15-30'
                        else '$30+' end as rev_p_mon
            , case when 12*coalesce(h.ltv,0) / a.tenure_mon <= 50 then '$0-50'
                        when 12*coalesce(h.ltv,0) / a.tenure_mon <= 100 then '$50-100'
                        when 12*coalesce(h.ltv,0) / a.tenure_mon <= 200 then '$100-200'
                        when 12*coalesce(h.ltv,0) / a.tenure_mon <= 400 then '$200-400'
                        else '$400+' end as rev_p_yr
            , case when a.tenure_mon < 4 then '0-3 Months'
                        when a.tenure_mon < 7 then '4-6 Months'
                        when a.tenure_mon < 13 then '7-12 Months'
                        when a.tenure_mon < 25 then '1-2 Years'
                        when a.tenure_mon < 37 then '2-3 Years'
                        when a.tenure_mon < 43 then '3-4 Years'
                        else '4+ Years' end as tenure
            , regexp_replace(a.sub_region,',',' ') sub_region
            , regexp_replace(a.region,',',' ') region
            , regexp_replace(f.first_name,',',' ') first_name
            , regexp_replace(f.last_name,',',' ') last_name
            , regexp_replace(f.phone1,',',' ') phone1
            , case when coalesce(f.gender,'n') = 'n' then '' else f.gender end gender
            , regexp_replace(f.city,',',' ') city
            , regexp_replace(f.state,',',' ') state
            , regexp_replace(f.zip,',',' ') zip
            , '' email
            , case when coalesce(d.eng_recent,99) < 8 then '0-7 days'
                        when coalesce(d.eng_recent,99) < 16 then '8-15 days'
                        when coalesce(d.eng_recent,99) < 31 then '16-30 days'
                        when coalesce(d.eng_recent,99) < 61 then '31-60 days'
                        when coalesce(d.eng_recent,99) < 91 then '61-90 days'
                        else '91+ days' end as eng_recent
            , case when coalesce(d.eng_tot,0) = 0 then 'None'
                        when coalesce(d.eng_tot,0) < 3 then '1-2 Times'
                        when coalesce(d.eng_tot,0) < 6 then '3-5 Times'
                        when coalesce(d.eng_tot,0) < 11 then '6-10 Times'
                        when coalesce(d.eng_tot,0) < 21 then '11-20 Times'
                        else '20+ Times' end as eng_tot
            , 'GODADDYCOM_138517' as surveyID
            , 'detractors@godaddy.com' as follow1
            , '' as contact
            , '' as contact_mgr
            , 'Active' as status
            , '' as launch_type
            , case when coalesce(e.act_wsb,0) = 1 then 'Yes' else 'No' end as wsb_active
            , case when coalesce(e.pub_wsb,0) = 1 then 'Yes' else 'No' end as wsb_pub
            , case when coalesce(e.act_ssl,0) = 1 then 'Yes' else 'No' end as ssl_active
            , case when coalesce(h.ltv,0) < 60 then '$0-60'
                    when coalesce(h.ltv,0) < 150 then '$60-150'
                    when coalesce(h.ltv,0) < 400 then '$150-400'
                    when coalesce(h.ltv,0) < 1000 then '$400-1000'
                    else '$1000+' end ltv
            , regexp_replace(h.last_chan,',',' ') last_chan
            , a.lclecd as lang
            , a.segment
            , case when a.prem_svc = 1 then 'Yes' else 'No' end as prem_svc
            , a.tenure_mon
            , case when a.isDoNotCall = 1 then 'Do Not Call' else 'Call eligible' end as dnc
            , regexp_replace(g.rev,',',' ') rev
            , regexp_replace(g.industry,',',' ') industry
            , '' as escal1
from    surv_shop a
left join surv_bill b
    on  a.shopper_id = b.shopper_id
left join surv_dom c
    on  a.shopper_id = c.shopper_id
left join surv_engage d
    on  a.shopper_id = d.shopper_id
left join surv_pms e
    on  a.shopper_id = e.shopper_id
left join surv_shopInfo f
    on  a.shopper_id = f.shopper_id
left join surv_survey g
    on  a.shopper_id = g.shopper_id
left join surv_ord h
    on  a.shopper_id = h.shopper_id
;