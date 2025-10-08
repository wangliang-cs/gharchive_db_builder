from db.mongodb_gh_count import GHArchiveMongoDBCountUtil
import config

count_db_util = GHArchiveMongoDBCountUtil(config.get_config("mongodb_conn_str"))

for year in range(2011, 2021):
    for month in range(1, 13):
        if year == 2020 and month >= 11:
            break
        count_db_util.count_events(f"events_id_{year}-{month:02d}", f"events_count_{year}-{month:02d}")
