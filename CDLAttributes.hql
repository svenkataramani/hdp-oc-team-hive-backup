SET hive.support.concurrency = false;
SET mapred.job.name = td_cdl_attribute_export;
USE hdp_oc_team;
DROP TABLE IF EXISTS tmp_cdl_attributes;
CREATE TABLE tmp_cdl_attributes row format delimited fields terminated by '|' STORED AS textfile AS
SELECT shopper_id
    ,cast(from_unixtime(unix_timestamp(t_XXX_projoindate)) as timestamp) as t_XXX_projoindate
    ,cast(from_unixtime(unix_timestamp(t_XXX_minclientadd)) as timestamp) as t_XXX_minclientadd
    ,cast(from_unixtime(unix_timestamp(t_XXX_signedat)) as timestamp) as t_XXX_signedat
    ,i_XXX_webproclients
    ,d_XXX_visit_eem
    ,d_xxx_visit_busreg
    ,d_xxx_visit_cal
    ,d_xxx_visit_cert_dom
    ,d_xxx_visit_dbp
    ,d_xxx_visit_ddt
    ,d_xxx_visit_dedhost
    ,d_xxx_visit_dom
    ,d_xxx_visit_email
    ,d_xxx_visit_hex
    ,d_xxx_visit_host
    ,d_xxx_visit_outright
    ,d_xxx_visit_qsc
    ,d_xxx_visit_traf_blaz
    ,d_xxx_visit_ssl
    ,d_xxx_visit_wsb
    ,d_xxx_visit_gf
    ,d_xxx_visit_merch
    ,d_xxx_visit_mktplc
    ,d_xxx_visit_office
    ,d_xxx_visit_home
    ,d_xxx_site_visits_30d
    ,d_xxx_site_visits_90d
    ,d_xxx_visit_mya
    ,d_xxx_mya_visits_30d
    ,d_xxx_mya_visits_90d
    ,i_xxx_abandon_office
    ,c_XXX_segmentcode
    ,c_XXX_segmentsourcecode
    ,b_XXX_nosegment
    ,b_XXX_nascent
    ,b_XXX_webpro
    ,b_XXX_moonlighter
    ,b_XXX_domainer
    ,b_XXX_personal
    ,b_XXX_ecommerce
    ,b_XXX_smallandhungry
    ,b_XXX_upandrunning
    ,b_XXX_established
    ,b_XXX_growthbusinesssizeunknown
    ,b_XXX_nonprofit
    ,b_XXX_modelv1default
    ,b_XXX_modelv1activebiz
    ,b_XXX_modelv1ebiz
    ,b_XXX_modelv1webpro
    ,b_XXX_modelv1domainer
    ,b_XXX_unknown
    ,b_XXX_other
    ,b_XXX_activebusiness
    ,b_XXX_onlinesellers
    ,b_XXX_webconsultants
    ,b_XXX_adomainer
    ,b_XXX_ncs_bus_np_own
    ,b_XXX_surv_bus_np_start
    ,b_XXX_ncs_bus_np_none
    ,b_XXX_ncs_bus_np_employ
    ,b_XXX_ncs_bus_accom_food
    ,b_XXX_ncs_bus_admin
    ,b_XXX_ncs_bus_agri
    ,b_XXX_ncs_bus_arts
    ,b_XXX_ncs_bus_construct
    ,b_XXX_ncs_bus_edu
    ,b_XXX_ncs_bus_finance
    ,b_XXX_ncs_bus_health
    ,b_XXX_ncs_bus_it
    ,b_XXX_ncs_bus_manuf
    ,b_XXX_ncs_bus_mine
    ,b_XXX_ncs_bus_prof
    ,b_XXX_ncs_bus_real_estate
    ,b_XXX_ncs_bus_retail
    ,b_XXX_ncs_bus_science
    ,b_XXX_ncs_bus_transpo
    ,b_XXX_ncs_bus_util
    ,b_XXX_ncs_bus_wholesale
    ,b_XXX_ncs_bus_other_prod
    ,b_XXX_ncs_bus_other_serv
    ,b_XXX_ncs_bus_legal
    ,b_XXX_ncs_bus_restaurant
    ,b_XXX_ncs_bus_nonprofit
    ,b_XXX_ncs_sell_website
    ,b_XXX_ncs_sell_3rd_party
    ,d_XXX_ncs_growth
    ,d_XXX_ncs_rev
    ,b_XXX_desc_ncs_web_pro
    ,b_XXX_ncs_desc_moonlight
    ,b_XXX_ncs_desc_domainer
    ,b_XXX_ncs_desc_none
    ,cast(from_unixtime(unix_timestamp(t_xxx_lastaccountvisit)) as timestamp) as t_xxx_lastaccountvisit
    ,i_xxx_accountvisits
    ,i_xxx_abandon_ddt
	,cast(from_unixtime(unix_timestamp()) as timestamp) as etl_datetime
	,b_xxx_ismidmarket
	,ols_ft_live_flg
	,cast(from_unixtime(unix_timestamp(ols_ft_strt_dt)) as timestamp) as ols_ft_strt_dt
	
FROM dbmarketing.cdl;