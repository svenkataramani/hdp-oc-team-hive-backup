SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

Insert Overwrite Table hdp_oc_team.customer_success_dashboard
select 
domain_id,
shopper_id,
email_providers,
has_free_email,
has_form,
has_googleanalytics,
has_login,
has_logo,
has_payment,
has_cart,
has_social,
naics3d,
naics2d,
naics3d_score,
naics2d_score,
has_facebook,
has_twitter,
has_googleplus,
has_tumblr,
has_instagram,
has_linkedin,
has_myspace,
was_cert_crawled
FROM bkamble.customer_success_dashboard;