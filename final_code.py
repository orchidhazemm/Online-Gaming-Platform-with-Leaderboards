from cassandra.cluster import Cluster
import redis
import json
#from uuid import uuid4
import uuid
from datetime import datetime

# Connect to Cassandra running in Docker
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('gaming')

# Insert data into player_profiles
def insert_player_profile(username, email, profile_picture, achievements, inventory, friends):
    session.execute(
        """
        INSERT INTO player_profiles (username, email, profile_picture, achievements, inventory, friends)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (username, email, profile_picture, achievements, inventory, friends)
    )
    
insert_player_profile('NOOR', 'NOOR@example.com', 'pic1.png', ['best player', 'best kill'], ['killer weapon', 'curved khinfe'], ['habiba', 'farida'])
insert_player_profile('ORCHID', 'ORCHID@example.com', 'pic2.png', ['most kills', 'high score'], ['grenade', 'knife'], ['toota', 'hana'])
insert_player_profile('SAMY', 'SAMY@example.com', 'pic3.png', ['most wins', 'best shooter'], ['sword', 'shot gun'], ['sherif', 'batreek'])


# Insert data into game_data
def insert_game_data(game_name, game_type, current_state, world_layout):
    session.execute(
        """
        INSERT INTO game_data (game_name, game_type, current_state, world_layout)
        VALUES (%s, %s, %s, %s)
        """,
        (game_name, game_type, current_state, world_layout)
    )
insert_game_data('Pubg', 'war', 'active', 'world1')

# Insert data into game_objects
def insert_game_object(object_id, object_type, position, attributes):
    session.execute(
        """
        INSERT INTO game_objects (object_id, object_type, position, attributes)
        VALUES (%s, %s, %s, %s)
        """,
        (object_id, object_type, position, attributes)
    )
insert_game_object(uuid.uuid4(), 'sword', 'x:50,y:20', {'damage': '60', 'weight': '5'})
insert_game_object(uuid.uuid4(), 'shot gun', 'x:100,y:45', {'damage': '100', 'weight': '7'})
insert_game_object(uuid.uuid4(), 'grenade', 'x:65,y:88', {'damage': '105', 'weight': '2'})


# Insert data into game_analytics
def insert_game_analytics(event_type, player_username, event_data):
    event_id = uuid.uuid4()
    timestamp = datetime.now()
    session.execute(
        """
        INSERT INTO game_analytics (event_id, event_type, player_username, event_data, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (event_id, event_type, player_username, event_data, timestamp)
    )
insert_game_analytics('kill', 'NOOR', {'item': 'killer weapon', 'description': 'A killer weapon'})
insert_game_analytics('item_pickup', 'ORCHID', {'item': 'grenade', 'description': 'explosion'})
insert_game_analytics('shoot', 'SAMY', {'item': 'shot gun', 'description': 'killer shot'})


    
# Function to insert/update player statistics
def insert_player_statistics(username, combat_stats, resource_stats, progression_stats):
    session.execute(
        """
        INSERT INTO player_statistics (username, combat_stats, resource_stats, progression_stats)
        VALUES (%s, %s, %s, %s)
        USING TTL 86400;
        """,
        (username, combat_stats, resource_stats, progression_stats)
    )
insert_player_statistics('NOOR', {'damage': 500, 'enemies_defeated': 5}, {'Guns': 20, 'bullets': 10}, {'level': '5', 'quests': ','.join(['shot', 'run'])})
insert_player_statistics('ORCHID', {'damage': 200, 'enemies_defeated': 3}, {'Guns': 40, 'bullets': 20}, {'level': '3', 'quests': ','.join(['shot', 'run'])})
insert_player_statistics('SAMY', {'damage': 300, 'enemies_defeated': 4}, {'Guns': 50, 'bullets': 10}, {'level': '4', 'quests': ','.join(['shot', 'run'])})

    
    
def get_player_stats(username):
    try:
        player_stats = session.execute(
            """
            SELECT combat_stats, resource_stats, progression_stats FROM player_statistics WHERE username=%s
            """, (username,)
        ).one()
        if player_stats:
            return {
                'username': username,
                'combat_stats': player_stats.combat_stats,
                'resource_stats': player_stats.resource_stats,
                'progression_stats': player_stats.progression_stats
            }
        else:
            return None
    except Exception as e:
        print(f"Cassandra error: {e}")
        return None

print("Getting player stats...\n")
print(get_player_stats('NOOR'))
print("\n")
print(get_player_stats('ORCHID'))
print("\n")
print(get_player_stats('SAMY'))
print("\n")


def get_player_achievements(username):
    try:
        player_profile = session.execute(
            """
            SELECT achievements FROM player_profiles WHERE username=%s
            """, (username,)
        ).one()
        if player_profile:
            return {
                'username': username,
                'achievements': player_profile.achievements
            }
        else:
            return None
    except Exception as e:
        print(f"Cassandra error: {e}")
        return None
    
print("Getting player achievments...\n")
print(get_player_achievements('NOOR'))
print(get_player_achievements('ORCHID'))
print(get_player_achievements('SAMY'))
print("\n")


def get_player_inventory(username):
    try:
        player_profile = session.execute(
            """
            SELECT inventory FROM player_profiles WHERE username=%s
            """, (username,)
        ).one()
        if player_profile:
            return {
                'username': username,
                'inventory': player_profile.inventory
            }
        else:
            return None
    except Exception as e:
        print(f"Cassandra error: {e}")
        return None

    
print("Getting player's inventory items...\n")
print(get_player_inventory('NOOR'))
print(get_player_inventory('ORCHID'))
print(get_player_inventory('SAMY'))
print("\n")
    
    
    
def get_friends_list(username):
    try:
        player_profile = session.execute(
            """
            SELECT friends FROM player_profiles WHERE username=%s
            """, (username,)
        ).one()
        if player_profile:
            return {
                'username': username,
                'friends': player_profile.friends
            }
        else:
            return None
    except Exception as e:
        print(f"Cassandra error: {e}")
        return None
    
print("Getting player's firends' list...\n")
print(get_friends_list('NOOR'))
print(get_friends_list('ORCHID'))
print(get_friends_list('SAMY'))
print("\n")





#Setup keyspace
# session.execute("""
# CREATE KEYSPACE IF NOT EXISTS gaming
# WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
# """)

# Setup tables in Cassandra
# session.execute("""
# CREATE TABLE IF NOT EXISTS player_profiles (
#     username TEXT PRIMARY KEY,
#     email TEXT,
#     profile_picture TEXT,
#     achievements LIST<TEXT>,
#     inventory LIST<TEXT>,
#     friends LIST<TEXT>
# )
# """)

# session.execute("""
# CREATE TABLE IF NOT EXISTS game_data (
#     game_name TEXT PRIMARY KEY,
#     game_type TEXT,
#     current_state TEXT,
#     world_layout TEXT
# )
# """)

# session.execute("""
# CREATE TABLE IF NOT EXISTS game_objects (
#     object_id UUID PRIMARY KEY,
#     object_type TEXT,
#     position TEXT,
#     attributes MAP<TEXT, TEXT>
# )
# """)

# session.execute("""
# CREATE TABLE IF NOT EXISTS game_analytics (
#     event_id UUID PRIMARY KEY,
#     event_type TEXT,
#     player_username TEXT,
#     event_data MAP<TEXT, TEXT>,
#     timestamp TIMESTAMP
# )
# """)

# CREATE TABLE IF NOT EXISTS player_statistics (
#     username TEXT PRIMARY KEY,
#     combat_stats MAP<TEXT, INT>,
#     resource_stats MAP<TEXT, INT>,
#     progression_stats MAP<TEXT, TEXT>
# );



# Connect to Redis running in Docker
redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)

# Implementing Redis functionalities with error handling
def update_player_location(player_id, x, y):
    location_data = {
        'x': x,
        'y': y,
        'timestamp': datetime.now().isoformat()
    }
    try:
        redis_client.rpush(f'player_location:{player_id}', json.dumps(location_data))
    except redis.exceptions.ResponseError as e:
        print(f"Caught an error: {e}")
        # Handle the error, e.g., by deleting the key if it's of the wrong type
        redis_client.delete(f'player_location:{player_id}')
        # Retry the operation
        redis_client.rpush(f'player_location:{player_id}', json.dumps(location_data))
        
update_player_location('NOOR', '888', '777')
update_player_location('ORCHID', '999', '666')
update_player_location('SAMY', '111', '222')


def log_game_event(player_name, event_type, details):
    event_data = {
        'type': event_type,
        'details': details,
        'timestamp': datetime.now().isoformat()
    }
    try:
        redis_client.rpush(f'game_events:{player_name}', json.dumps(event_data))
    except redis.exceptions.ResponseError as e:
        print(f"Caught an error: {e}")
        
log_game_event('NOOR', 'kill', 'killed a person')
log_game_event('ORCHID', 'explosion', 'threw a grenade')
log_game_event('SAMY', 'shoot', 'shot a person')




def send_chat_message(guild_id, player_name, message):
    
    chat_message = {
        'player_name': player_name,
        'message': message,
        'timestamp': datetime.now().isoformat()
    }
    try:
        redis_client.rpush(f'chat_messages:{guild_id}', json.dumps(chat_message))
    except redis.exceptions.RedisError as e:
        print(f"Redis error: {e}")
        
send_chat_message('guild1', 'NOOR', 'Hello team!')
send_chat_message('guild1', 'NOOR', 'Where are everyone?')
send_chat_message('guild1', 'NOOR', 'HELLO')

send_chat_message('guild2', 'ORCHID', 'Send Help!')
send_chat_message('guild2', 'ORCHID', 'Icecream!')

send_chat_message('guild3', 'SAMY', 'Follow me!')
send_chat_message('guild3', 'SAMY', 'Get in the car')


def get_chat_messages(guild_id, count=10):
    
    try:
        messages = redis_client.lrange(f'chat_messages:{guild_id}', -count, -1)
        return [json.loads(message) for message in messages]
    except redis.exceptions.RedisError as e:
        print(f"Redis error: {e}")
        return []
    
print("Getting chat history...\n")
print(get_chat_messages('guild1', 2))
print("\n")
        
        
def update_leaderboard(player_id, metric, score):
    
    try:
        # Increment the score for cumulative metrics like points and wins
        if metric in ['points', 'wins']:
            redis_client.zincrby(f'leaderboard:{metric}', score, player_id)
        # Set a new score for metrics like completion time where the latest value is what matters
        elif metric == 'completion_time':
            redis_client.zadd(f'leaderboard:{metric}', {player_id: score})
    except redis.exceptions.RedisError as e:
        print(f"Redis error: {e}")

update_leaderboard('NOOR', 'points', 1800)
update_leaderboard('ORCHID', 'points', 200)
update_leaderboard('SAMY', 'points', 1500)

update_leaderboard('NOOR', 'wins', 50)
update_leaderboard('ORCHID', 'wins', 20)
update_leaderboard('SAMY', 'wins', 40)

update_leaderboard('NOOR', 'completion_time', 8800)
update_leaderboard('ORCHID', 'completion_time', 7200)
update_leaderboard('SAMY', 'completion_time', 7600)
        
        
def get_current_leaderboard(metric, top_n=10):
        try:
            leaderboard = redis_client.zrevrange(f'leaderboard:{metric}', 0, top_n - 1, withscores=True)
            return [(player_id.decode('utf-8'), score) for player_id, score in leaderboard]
        except redis.exceptions.RedisError as e:
            print(f"Redis error: {e}")
            return []
        
print("Getting current leaderboard...\n")
print("POINTS:")
print(get_current_leaderboard('points', 3))
print("\n")
print("WINS:")
print(get_current_leaderboard('wins', 3))
print("\n")
print("COMPLETION TIME:")
print(get_current_leaderboard('completion_time', 3))
print("\n")



def get_recent_game_events(player_name, count=10):
    try:
        events = redis_client.lrange(f'game_events:{player_name}', -count, -1)
        return [json.loads(event) for event in events]
    except redis.exceptions.RedisError as e:
        print(f"Redis error: {e}")
        return []
    
print("Getting recent game events...\n")
print(get_recent_game_events('NOOR', 1))
print("\n")


def get_player_rank(player_id, metric):
    try:
        rank = redis_client.zrevrank(f'leaderboard:{metric}', player_id)
        if rank is not None:
            # Redis ranks are 0-based, so add 1 to make it 1-based
            return {'username': player_id, 'metric': metric, 'rank': rank + 1}
        else:
            return {'username': player_id, 'metric': metric, 'rank': None}
    except redis.exceptions.RedisError as e:
        print(f"Redis error: {e}")
        return {'username': player_id, 'metric': metric, 'rank': None}

    
print("Getting player rank based on metric... \n")
print(get_player_rank('NOOR', 'points'))
print(get_player_rank('ORCHID', 'wins'))
print(get_player_rank('SAMY', 'completion_time'))
