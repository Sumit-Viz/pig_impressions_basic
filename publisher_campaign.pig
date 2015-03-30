/* publisher_campaign.pig*/

/* 1. get publisher ids
2. get campaign ids
3. get impression counts per campaign,publisher_id
Inputs : 
1. impression logs 2. zoneidToPublId mapping 3. bannerid to campaignId mapping
*/

/* Impressions Path */
%default IMPRESSIONS_PATH 's3://pathToImpressionsLog/DailyImpressions_CurDate.csv'
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