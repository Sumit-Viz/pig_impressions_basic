Impressions = load 's3://viz-raw-data/cost-impr/2015/03/30' using PigStorage('\t');
ZonePublMap = load 's3://viz-db-dump/zoneidtopublid.csv' using PigStorage('\t');
BannerCampaignMap = load 's3://viz-db-dump/banneridtoadvid.csv' using PigStorage('\t');

I = foreach Impressions generate $0 as cookie,$3 as bannerid,$5 as zoneid,$32 as imprid;

i_zp = join I by $2, ZonePublMap by $0;

i_zp_ba = join i_zp by $1, BannerCampaignMap by $0;

i_grp = group i_zp_ba by ($5,$7);

i_grp_cnt = foreach i_grp {
unique_users = distinct $0;
generate group,COUNT(unique_users) as user_cnt,COUNT(i_zp_ba) as num_impressions;
};

STORE i_grp_cnt INTO 's3://viz-temp/sumit/impressions/' using PigStorage('\t');

clicks = load 's3://viz-raw-data/kafka-click/' using PigStorage('\t');

C = foreach clicks generate $0 as cookie,$1 as imprid,$3 as bannerid,$5 as publid;

c_zp = join C by $3, ZonePublMap by $0;

c_zp_ba = join c_zp by $2, BannerCampaignMap by $0;

c_grp = group c_zp_ba by ($5,$7);

c_grp_cnt = foreach c_grp {
unique_users = distinct $0;
generate group,COUNT(unique_users) as user_cnt,COUNT(c_zp_ba) as num_clicks;
};

STORE c_grp_cnt INTO 's3://viz-temp/sumit/counts/' using PigStorage('\t');
