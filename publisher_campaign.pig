/* publisher_campaign.pig*/

/* 1. get publisher ids
2. get campaign ids
3. get impression counts per campaign,publisher_id
Inputs : 
1. impression logs 2. zoneidToPublId mapping 3. bannerid to campaignId mapping
*/

/* Impressions Path */
%default IMPRESSIONS_PATH 's3://pathToImpressionsLog/DailyImpressions_CurDate.csv'
%default CLICKS_PATH = 's3://pathToClicksLog/DailyClicks_CurDate.csv'
%default SALES_PATH = 's3://pathToSalesLog/DailySales_CurDate.csv'
/* output path(s) */
%default OUTPUT_PATH_IU 's3://pathToCOUNT/imp_users_P_C.csv'
%default OUTPUT_PATH_SC 's3://pathToCOUNT/sales_clicks_P_C.csv'
/* Mappings Path */
%default ZONE_PUB_PATH 's3://pathToMappings/ZoneidToPublId.csv'
%default BANNER_CAMP_PATH 's3://pathToMappings/BanneridToCampaignid.csv'
/* columns settings */
%default ImpZoneCol 'ZoneId'
%default ZoneCol 'ZoneId'

%default ImpBannerCol 'BannerId'
%default BannerCol 'BannerId'

%default CampaignId 'CampaignId'
%default PublId 'PublId'

%default Impressions 'Num_Impressions'
%default b_cookie 'B_COOKIE'

%default $IID 'impression_id'
%default Clicks 'Num_Clicks'

%default S_id 'sales_id'
%default Sales 'Num_Sales'

/* Pig Latin Statements */
/* Impressions Log */
Impressions = load '$IMPRESSIONS_PATH' using PigStorage();
/* Zone<->Publisher */
ZonePublMap = load '$ZONE_PUB_PATH' using PigStorage();
/* Banner<->Campaign */
BannerCampaignMap = load '$BANNER_CAMP_PATH' using PigStorage();
/* could have joined three tables all at once, either */
ImpPubl = join Impressions by $ImpZoneCol, ZonePublMap by $ZoneCol;
ImpPublCamp = join ImpPubl by $ImpBannerCol, BannerCampaignMap by $BannerCol;
/* group by the campaignId and PublId*/
Grpd = GROUP ImpPublCamp by $CampaignId,$PublId;
/* have a look at the Grpd first */
DUMP Grpd;
/* select count of Impressions,Distinct Users for a P,C all at once */
Grpd_Cnt = FOREACH Grpd GENERATE CampaignId,PublId,COUNT($Impressions),COUNT(DISTINCT $b_cookie);
STORE Grpd_Cnt INTO $OUTPUT_PATH_IU

/* getting the clicks and sales for impressions now */
Clicks = load '$CLICKS_PATH' using PigStorage();
/* join imp_publ_campaign with clicks too! */
Imp_Clicks = join ImpPublCamp by $IID, Clicks by $IID;
/* group 'em by pub,camp */
Grpd_Imp_Clicks = GROUP Imp_Clicks by $CampaignId,$PublId;
DUMP Grpd_Imp_Clicks;
/* get count of clicks */
Grpd_Clicks_Cnt = FOREACH Grpd_Imp_Clicks GENERATE CampaignId,PublId,COUNT($Clicks);

/* getting the sales for impressions now */
Sales = load '$SALES_PATH' using PigStorage();
/* we now 'd have imp,clicks,sales,camp,publ */
Imp_Clicks_Sales = join Imp_Clicks by $S_id,Sales by $S_id;
/* group 'em by pub,camp */
Grpd_Imp_Clicks_Sales = GROUP Imp_Clicks_Sales by $CampaignId,$PublId;
DUMP Grpd_Imp_Clicks_Sales;
/* get count of sales */
Grpd_Sales_Cnt = FOREACH Grpd_Imp_Clicks_Sales GENERATE CampaignId,PublId,COUNT($Sales);
/* store the output */
STORE Grpd_Sales_Cnt INTO $OUTPUT_PATH_SC;
