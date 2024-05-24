CREATE KEYSPACE IF NOT EXISTS GamePlatform
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
        };
        
CREATE TABLE IF NOT EXISTS GamePlatform.player_profiles (
    player_id UUID PRIMARY KEY,
    username TEXT,
    email TEXT,
    profile_picture TEXT,
    achievements LIST<TEXT>,
    inventory MAP<TEXT, INT>,
    friend_list LIST<UUID>
);

CREATE TABLE IF NOT EXISTS GamePlatform.game_info (
    game_id UUID PRIMARY KEY,
    name TEXT,
    type TEXT,
    current_state TEXT,
    world_layout_details TEXT
);

CREATE TABLE IF NOT EXISTS GamePlatform.game_objects (
    game_id UUID,
    object_id UUID,
    type TEXT,
    position TEXT,
    attributes MAP<TEXT, TEXT>,  -- key-value pairs of attributes
    PRIMARY KEY ((game_id), object_id)  -- Composite partition key on game_id, clustering key on object_id
);

CREATE TABLE IF NOT EXISTS GamePlatform.chat_history (
    chat_id UUID,
    message_id TIMEUUID,  // Use TIMEUUID for unique message identifiers that sort chronologically
    sender_id UUID,
    message_text TEXT,
    timestamp TIMESTAMP,
    PRIMARY KEY ((chat_id), timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS GamePlatform.player_combat_stats (
    player_id UUID,
    game_id UUID,
    total_damage_dealt INT,
    enemies_defeated INT,
    last_update TIMESTAMP,
    PRIMARY KEY (player_id, game_id)
);

CREATE TABLE IF NOT EXISTS GamePlatform.player_behavior (
    player_id UUID,
    game_id UUID,
    event_time TIMESTAMP,
    action TEXT,
    details TEXT,
    PRIMARY KEY ((player_id, game_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);