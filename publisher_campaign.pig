/* publisher_campaign.pig*/

/* 1. get publisher ids
2. get campaign ids
3. get impression counts per campaign,publisher_id
4. other stuff
Inputs : 
1. impression logs 2. zoneidToPublId mapping 3. bannerid to campaignId mapping
*/

/* ======================================================================================================================================== */

/* Impressions Path */
%default IMPRESSIONS_PATH 's3://pathToImpressionsLog/DailyImpressions_CurDate.csv'

/* Clicks Path */
%default CLICKS_PATH = 's3://pathToClicksLog/DailyClicks_CurDate.csv'

/* Sales Path */
%default SALES_PATH = 's3://pathToSalesLog/DailySales_CurDate.csv'

/* output path(s) */
%default OUTPUT_PATH_IU 's3://pathToCOUNT/imp_users_P_C.csv'
%default OUTPUT_PATH_SC 's3://pathToCOUNT/sales_clicks_P_C.csv'
%default OUTPUT_FINAL_FUNNEL 's3://pathToCOUNT/funnel_P_C.csv'

/* Mappings Path */
%default ZONE_PUB_PATH 's3://pathToMappings/ZoneidToPublId.csv'
%default BANNER_CAMP_PATH 's3://pathToMappings/BanneridToCampaignid.csv'
%default BANNER_CLASS_PATH 's3://pathToMappings/Banneridtobannerclass.csv'

/* columns settings */
%default ImpZoneCol 'zoneid'
%default ZoneCol 'zone_id'

%default ImpBannerCol 'bannerid'
%default BannerCol 'banner_id'

%default CampaignId 'adv_id'
%default PublId 'publ_id'

%default b_cookie 'cookie'

%default $IID 'ImprID'

/* ======================================================================================================================================== */

/* Pig Latin Statements */

/* Impressions Log */
Impressions = load '$IMPRESSIONS_PATH' using PigStorage();

/* Zone<->Publisher */
ZonePublMap = load '$ZONE_PUB_PATH' using PigStorage();

/* Banner<->Campaign */
BannerCampaignMap = load '$BANNER_CAMP_PATH' using PigStorage();

/* could have joined three tables all at once, either */
ImpPubl = join Impressions by $ImpZoneCol, ZonePublMap by $ZoneCol;
ImpPublCamp = join ImpPubl by $ImpBannerCol, BannerCampaignMap by $BannerCol;/* have both P and C now */

/* group by the campaignId and PublId*/
Grpd = GROUP ImpPublCamp by $CampaignId,$PublId;

/* have a look at the Grpd first */
DUMP Grpd;
/* select count of Impressions,Distinct Users for a P,C all at once */

Grpd_Cnt = FOREACH Grpd GENERATE $CampaignId,$PublId,COUNT(ImpPublCamp) AS num_impressions,COUNT(DISTINCT $b_cookie) AS distinct_users;
Grpd_Cnt_g = FOREACH Grpd_Cnt GENERATE $CampaignId,$PublId,distinct_users,num_impressions;
STORE Grpd_Cnt INTO $OUTPUT_PATH_IU /* Grpd_Cnt is important to extract distinct users and num_impressions */ 

/* getting the clicks and sales for impressions now */
Clicks = load '$CLICKS_PATH' using PigStorage();

/* join imp_publ_campaign with clicks too! */
Imp_Clicks = join ImpPublCamp by ($IID,$b_cookie), Clicks by ($IID,$b_cookie);/* because IID may corresponding to two same but different user impressions ? */

/* group 'em by pub,camp */
Grpd_Imp_Clicks = GROUP Imp_Clicks by $CampaignId,$PublId;
DUMP Grpd_Imp_Clicks;

/* get count of clicks */
Grpd_Clicks_Cnt = FOREACH Grpd_Imp_Clicks GENERATE $CampaignId,$PublId,COUNT(Imp_Clicks) AS num_clicks;
Grpd_Clicks_Cnt_g = FOREACH Grpd_Clicks_CNT GENERATE $CampaignId,$PublId,num_clicks;

/* getting the sales for impressions now */
Sales = load '$SALES_PATH' using PigStorage();

/* load banner class mapping */
BannerClassMap = LOAD '$BANNER_CLASS_PATH' using PigStorage();

/* to map sales to the adv */
Sales_Banner = JOIN Sales by bannerclassname,BannerClassMap by banner_class;
Sales_Adv = JOIN Sales_Banner by banner_id,BannerCampaignMap by banner_id;

/* we now 'd have imp,clicks,sales,camp,publ */
Imp_Clicks_Sales = join Imp_Clicks by (cookie,adv_id,publ_id),Sales_Adv by (cookie,adv_id,publ_id);

/* group 'em by pub,camp */
Grpd_Imp_Clicks_Sales = GROUP Imp_Clicks_Sales by $CampaignId,$PublId;

/* a brisk glimpse of the data */
DUMP Grpd_Imp_Clicks_Sales;

/* get count of sales */
Grpd_Sales_Cnt = FOREACH Grpd_Imp_Clicks_Sales GENERATE $CampaignId,$PublId,COUNT(Imp_Clicks_Sales) AS num_sales;
Grpd_Sales_Cnt_g = FOREACH Grpd_Sales_Cnt GENERATE $CampaignId,$PublId,num_sales;

/* store the output */
STORE Grpd_Sales_Cnt INTO $OUTPUT_PATH_SC;

/* ======================================================================================================================================== */

/* get them all as a funnel now */

funnel = JOIN Grpd_Cnt_g by ($CampaignId,$PublId),Grpd_Clicks_Cnt_g by ($CampaignId,$PublId),Grpd_Sales_Cnt_g by ($CampaignId,PublId);
STORE funnel into $OUTPUT_FINAL_FUNNEL;
