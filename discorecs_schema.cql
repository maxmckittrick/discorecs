
CREATE KEYSPACE discorecs WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

CREATE TABLE discorecs.releases_scored (
    release_id int PRIMARY KEY,
    artist text,
    blues_score int,
    brass_score int,
    childrens_score int,
    classical_score int,
    electronic_score int,
    folk_score int,
    funk_score int,
    genre text,
    hiphop_score int,
    jazz_score int,
    latin_score int,
    nonmusic_score int,
    pop_score int,
    reggae_score int,
    rock_score int,
    stagescreen_score int,
    title text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE discorecs.users (
    user_id ascii PRIMARY KEY,
    blues_score int,
    brass_score int,
    childrens_score int,
    classical_score int,
    collection list<int>,
    electronic_score int,
    folk_score int,
    funk_score int,
    hiphop_score int,
    jazz_score int,
    latin_score int,
    nonmusic_score int,
    pop_score int,
    recommended_release int,
    recommended_user ascii,
    reggae_score int,
    rock_score int,
    stagescreen_score int
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE discorecs.releases (
    release_id int PRIMARY KEY,
    artist text,
    blues_score int,
    brass_score int,
    childrens_score int,
    classical_score int,
    electronic_score int,
    folk_score int,
    funk_score int,
    genre text,
    hiphop_score int,
    jazz_score int,
    latin_score int,
    nonmusic_score int,
    pop_score int,
    reggae_score int,
    rock_score int,
    stagescreen_score int,
    title text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE discorecs.users_scored (
    user_id ascii PRIMARY KEY,
    blues_score int,
    brass_score int,
    childrens_score int,
    classical_score int,
    collection list<int>,
    electronic_score int,
    folk_score int,
    funk_score int,
    hiphop_score int,
    jazz_score int,
    latin_score int,
    nonmusic_score int,
    pop_score int,
    recommended_release int,
    recommended_user ascii,
    reggae_score int,
    rock_score int,
    stagescreen_score int
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

