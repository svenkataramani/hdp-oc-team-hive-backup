SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

WITH
dcr_wsb as
(
	select dcr.shopper_id, er.resource_id, case when dcr.e_id = 'pandc.wsb.v7.publish' then 'Publish' else 'Activate' end as event_action, 'WSB' as prod_type, max(e_time) as event_date
	  from dm_ecommerce.fact_billing_external_resource er
	  join 
	  (select * from dm_customer_interaction.dm_user_events dcr
	   where e_id in('pandc.wsb.v7.theme.select','pandc.wsb.v7.publish')
	   
	   AND dcr.year   = year(TO_DATE('${hiveconf:startdate}'))
	   AND dcr.month  >= month(TO_DATE('${hiveconf:startdate}'))
			
	   ) dcr on er.external_resource_id = dcr.orion_id
	   WHERE regexp_extract(dcr.shopper_id,'[a-zA-Z]+', 0) = ''
	 group by dcr.shopper_id, er.resource_id, case when dcr.e_id = 'pandc.wsb.v7.publish' then 'Publish' else 'Activate' end, 'WSB'
),

dcr_ols as
(
-- ADDED 5/16/16.  ALTERNATE SOURCE FOR OLS EVENTS; TESTING FOR BETTER DATA QUALITY
	select a.shopper_id, o.resource_id, case when m.value = 'ONBOARDING' then 'Acct Activate' else 'Publish' end as event_action, 'OLS' as prod_type, max(m.created_at) as event_date
	from 
	nemo_ols_common.nemo_core_account_metrics_snap m
	inner join
	nemo_ols_common.nemo_core_site_accounts_snap a on a.id = m.site_account_id
	inner join
	dm_ecommerce.fact_billing_external_resource o on a.orion_id = o.external_resource_id
	where m.event = 'store_status.change'
	and m.value in('ONBOARDING','LIVE')
	and a.shopper_id rlike '^[0-9]+$' = TRUE
	group by a.shopper_id, o.resource_id, case when m.value = 'ONBOARDING' then 'Acct Activate' else 'Publish' end, 'OLS'

-- DCR CODE BELOW, REMOVED DUE TO DATA ISSUES. DO NOT DELETE
-- select dcr.shopper_id, er.resource_id, case when dcr.e_id = 'pandc.ols.account.activate' then 'Acct Activate' when dcr.e_id like 'pandc.ols.add_on.activate%' then 'AddOn Activate' when dcr.e_id = 'pandc.ols.store_status.change' then 'Publish' end as event_action,'OLS' as prod_type, max(e_time) as event_date
--  from dm_ecommerce.fact_billing_external_resource er
--  join 
--  (select * from dm_customer_interaction.dm_user_events dcr
--   where (e_id = 'pandc.ols.account.activate' or e_id like 'pandc.ols.add_on.activate%' or (e_id = 'pandc.ols.store_status.change' and event_json like '{"new_value":"LIVE",%'))) dcr on er.external_resource_id = dcr.orion_id
--   WHERE regexp_extract(dcr.shopper_id,'[a-zA-Z]+', 0) = ''
-- group by dcr.shopper_id, er.resource_id, case when dcr.e_id = 'pandc.ols.account.activate' then 'Acct Activate' when dcr.e_id like 'pandc.ols.add_on.activate%' then 'AddOn Activate' when dcr.e_id = 'pandc.ols.store_status.change' then 'Publish' end, 'OLS'
),

dcr_sev as
(
	select dcr.shopper_id, er.resource_id, case when dcr.e_id = 'pandc.sev.review.published' then 'Publish' else 'Activate' end as event_action, 'SEV' as prod_type, max(e_time) as event_date
	  from dm_ecommerce.fact_billing_external_resource er
	  join 
	  (select * from dm_customer_interaction.dm_user_events dcr
	   where e_id in('pandc.sev.account.create','pandc.sev.review.published')
	  
	   AND dcr.year   = year(TO_DATE('${hiveconf:startdate}'))
	   AND dcr.month  >= month(TO_DATE('${hiveconf:startdate}'))
		   
	   and get_json_object(event_json, '$.product_mode') = 'standalone') dcr 
	   on er.external_resource_id = dcr.orion_id
	   WHERE regexp_extract(dcr.shopper_id,'[a-zA-Z]+', 0) = ''
	 group by dcr.shopper_id, er.resource_id, case when dcr.e_id = 'pandc.sev.review.published' then 'Publish' else 'Activate' end, 'SEV'
),

dcr_mwp as
(
--OLD MWP CODE REMOVED DUE TO DATA ISSUES - 10/4/2016
--select sr.shopper_id, sr.shopperresourceid as resource_id, case when mi.metricid = 459 then 'Publish' else 'Activate' end as event_action, 'MWP' as prod_type, max(sequencedategmt) as event_date
--FROM productmetricscoring.score_metricinstance_snap mi
--JOIN productmetricscoring.score_shopperresource_snap sr ON mi.shopperresourceid = sr.shopperresourceid
--where metricid in(459,581)
--AND regexp_extract(sr.shopper_id,'[a-zA-Z]+', 0) = ''
--group by sr.shopper_id, sr.shopperresourceid, case when mi.metricid = 459 then 'Publish' else 'Activate' end, 'MWP'

	select u.shopperid as shopper_id, r.resource_id, case when f.fastballmetricsid = 5 then 'Publish' else 'Activate' end as event_action, 'MWP' as prod_type, max(f.metricvalue) as event_date
	from
	hosting_wpaas.fastballmessages_snap f
	inner join
	hosting_wpaas.accounts_snap a on f.accountid = a.accountid
	inner join
	hosting_wpaas.userprofiles_snap u on a.userid = u.userid
	inner join
	dm_ecommerce.fact_billing_external_resource r on a.accountuid = r.external_resource_id
	where f.fastballmetricsid in (5,3)
	and regexp_extract(u.shopperid,'[a-zA-Z]+', 0) = ''
	group by u.shopperid, r.resource_id, case when f.fastballmetricsid = 5 then 'Publish' else 'Activate' end, 'MWP'

	union all

	Select up.shopperid as shopper_id, br.resource_id, 'ezMode' as event_action, 'MWP' as prod_type, max(to_date(get_json_object(data, '$.datetime'))) as event_date
	FROM hosting_wpaas.userprofiles_snap up
	JOIN hosting_wpaas.products_snap pr on pr.userid=up.userid
	JOIN hosting_wpaas.accounts_snap ac on ac.productid=pr.productid
	JOIN hosting_wpaas.pluginaccountdatas_snap pad on pad.accountid=ac.accountid
	JOIN godaddybilling.gdshop_billingaccountexternalresource_snap br on ac.accountuid = br.externalresourceid
	WHERE LOWER(data) like '%wpem_continue%yes%'
	AND regexp_extract(up.shopperid,'[a-zA-Z]+', 0) = ''
	group by up.shopperid, br.resource_id, 'ezMode', 'MWP'
	union all
	select u.shopperid as shopper_id, r.resource_id, 'Migrate' as event_action, 'MWP' as prod_type, min(m.datecreated) as event_date
	from
	hosting_wpaas.migrations_snap m
	inner join
	hosting_wpaas.accounts_snap a on m.accountid = a.accountid
	inner join
	hosting_wpaas.userprofiles_snap u on a.userid = u.userid
	inner join
	hosting_wpaas.products_snap p on a.productid = p.productid
	inner join
	dm_ecommerce.fact_billing_external_resource r on p.productuid = r.external_resource_id
	where regexp_extract(u.shopperid,'[a-zA-Z]+', 0) = ''
	and to_date(m.datecreated) >= '2014-01-07'
	group by u.shopperid, r.resource_id, 'Migrate', 'MWP'
),

dcr_gem as
(
	select dcr.shopper_id, er.resource_id, case when dcr.e_id = 'productivity.gem.dashboard.email.send' then 'Gem Send' else 'inactive' end as event_action, 'GEM' as prod_type, max(e_time) as event_date
	  from dm_ecommerce.fact_billing_external_resource er
	  join 
	  (select * from dm_customer_interaction.dm_user_events dcr
	   where e_id in('productivity.gem.dashboard.email.send')
	   
	   AND dcr.year   = year(TO_DATE('${hiveconf:startdate}'))
	   AND dcr.month  >= month(TO_DATE('${hiveconf:startdate}'))
			
	   ) dcr
	   on er.external_resource_id = dcr.orion_id
	  WHERE regexp_extract(dcr.shopper_id,'[a-zA-Z]+', 0) = '' 
	 group by dcr.shopper_id, er.resource_id, case when dcr.e_id = 'productivity.gem.dashboard.email.send' then 'Gem Send' else 'inactive' end, 'GEM'
),

dcr_cpnl as
(
	select dcr.shopper_id, er.resource_id, case when dcr.e_id = 'hosting.cpanel.api.customer_migration.complete' then 'Migrate' when dcr.e_id = 'hosting.cpanel.account.setup.complete' then 'Setup' else 'Publish' end as event_action, 'cPanel' as prod_type, max(e_time) as event_date
	  from dm_ecommerce.fact_billing_external_resource er
	  join 
	  (select * from dm_customer_interaction.dm_user_events dcr
	   where e_id in('hosting.cpanel.api.customer_migration.complete', 'hosting.cpanel.account.setup.complete', 'hosting.cpanel.account.publish')
	   
	   AND dcr.year   = year(TO_DATE('${hiveconf:startdate}'))
	   AND dcr.month  >= month(TO_DATE('${hiveconf:startdate}'))
			
	   ) dcr
	   on er.external_resource_id = dcr.orion_id
	  WHERE regexp_extract(dcr.shopper_id,'[a-zA-Z]+', 0) = '' 
	 group by dcr.shopper_id, er.resource_id, case when dcr.e_id = 'hosting.cpanel.api.customer_migration.complete' then 'Migrate' when dcr.e_id = 'hosting.cpanel.account.setup.complete' then 'Setup' else 'Publish' end, 'cPanel'
),

dcr_gf as
(
	SELECT gd.shopper_id, otm.resource_id, 'ACTIVATE' as event_action, 'GF' as prod_type, max(activation_date) as event_date
	FROM
	bisandbox.productusage_sev p
	JOIN
	dm_ecommerce.fact_billing_external_resource otm ON LOWER(p.orion_id) = LOWER(otm.external_resource_id) AND otm.product_type_id = 468
	JOIN
	dm_ecommerce.fact_billing gd ON otm.resource_id = gd.resource_id AND otm.product_type_id = gd.product_type_id
	WHERE activation_date IS NOT NULL
	AND regexp_extract(gd.shopper_id,'[a-zA-Z]+', 0) = ''
	GROUP BY gd.shopper_id, otm.resource_id, 'ACTIVATE'
	
	UNION ALL
	
	SELECT gd.shopper_id, otm.resource_id, 'PUBLISH' as event_action, 'GF' as prod_type, max(publication_date) as event_date
	FROM
	bisandbox.productusage_sev p
	JOIN
	dm_ecommerce.fact_billing_external_resource otm ON LOWER(p.orion_id) = LOWER(otm.external_resource_id) AND otm.product_type_id = 468
	JOIN
	dm_ecommerce.fact_billing gd ON otm.resource_id = gd.resource_id AND otm.product_type_id = gd.product_type_id
	WHERE publication_date IS NOT NULL
	AND regexp_extract(gd.shopper_id,'[a-zA-Z]+', 0) = ''
	GROUP BY gd.shopper_id, otm.resource_id, 'PUBLISH'
),

dcr_union as
(
	select dcr_wsb.shopper_id, dcr_wsb.resource_id, dcr_wsb.prod_type, to_date(dcr_wsb.event_date) as event_date, dcr_wsb.event_action
	from dcr_wsb
	union all
	select dcr_ols.shopper_id, dcr_ols.resource_id, dcr_ols.prod_type, to_date(dcr_ols.event_date) as event_date, dcr_ols.event_action
	from dcr_ols
	union all
	select dcr_mwp.shopper_id, dcr_mwp.resource_id, dcr_mwp.prod_type, to_date(dcr_mwp.event_date) as event_date, dcr_mwp.event_action
	from dcr_mwp
	union all
	select dcr_sev.shopper_id, dcr_sev.resource_id, dcr_sev.prod_type, to_date(dcr_sev.event_date) as event_date, dcr_sev.event_action
	from dcr_sev
	union all
	select dcr_gem.shopper_id, dcr_gem.resource_id, dcr_gem.prod_type, to_date(dcr_gem.event_date) as event_date, dcr_gem.event_action
	from dcr_gem
	union all
	select dcr_cpnl.shopper_id, dcr_cpnl.resource_id, dcr_cpnl.prod_type, to_date(dcr_cpnl.event_date) as event_date, dcr_cpnl.event_action
	from dcr_cpnl
	union all
	select dcr_gf.shopper_id, dcr_gf.resource_id, dcr_gf.prod_type, to_date(dcr_gf.event_date) as event_date, dcr_gf.event_action
	from dcr_gf
)

insert overwrite table prod_events
select dcr_union.shopper_id, dcr_union.resource_id, dcr_union.prod_type, to_date(dcr_union.event_date) as event_date, dcr_union.event_action
from dcr_union
group by dcr_union.shopper_id, dcr_union.resource_id, dcr_union.prod_type, dcr_union.event_date, dcr_union.event_action
;