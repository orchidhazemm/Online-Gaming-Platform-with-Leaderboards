from cassandra.cluster import Cluster
import redis
import json
from uuid import uuid4
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

# Insert data into game_data
def insert_game_data(game_name, game_type, current_state, world_layout):
    session.execute(
        """
        INSERT INTO game_data (game_name, game_type, current_state, world_layout)
        VALUES (%s, %s, %s, %s)
        """,
        (game_name, game_type, current_state, world_layout)
    )

# Insert data into game_objects
def insert_game_object(object_id, object_type, position, attributes):
    session.execute(
        """
        INSERT INTO game_objects (object_id, object_type, position, attributes)
        VALUES (%s, %s, %s, %s)
        """,
        (object_id, object_type, position, attributes)
    )

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

# Example usage
insert_player_profile('NOOR', 'NOOR@example.com', 'pic1.png', ['best player', 'best kill'], ['killer weapon', 'curved khinfe'], ['habiba', 'farida'])
insert_player_profile('ORCHID', 'ORCHID@example.com', 'pic2.png', ['most kills', 'high score'], ['grenade', 'knife'], ['toota', 'hana'])
insert_player_profile('SAMY', 'SAMY@example.com', 'pic3.png', ['most wins', 'best shooter'], ['sword', 'shot gun'], ['sherif', 'batreek'])
insert_game_data('Pubg', 'war', 'active', 'world1')
insert_game_object(uuid.uuid4(), 'sword', 'x:50,y:20', {'damage': '60', 'weight': '5'})
insert_game_object(uuid.uuid4(), 'shot gun', 'x:100,y:45', {'damage': '100', 'weight': '7'})
insert_game_object(uuid.uuid4(), 'grenade', 'x:65,y:88', {'damage': '105', 'weight': '2'})
insert_game_analytics('kill', 'NOOR', {'item': 'killer weapon', 'description': 'A killer weapon'})
insert_game_analytics('item_pickup', 'ORCHID', {'item': 'grenade', 'description': 'explosion'})
insert_game_analytics('shoot', 'SAMY', {'item': 'shot gun', 'description': 'killer shot'})



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

def update_leaderboard(player_id, score):
    try:
        redis_client.zadd('leaderboard:points', {player_id: score})
    except redis.exceptions.ResponseError as e:
        print(f"Caught an error: {e}")

def send_chat_message(guild_id, player_name, message):
    chat_message = {
        'player_name': player_name,
        'message': message,
        'timestamp': datetime.now().isoformat()
    }
    try:
        redis_client.rpush(f'chat_messages:{guild_id}', json.dumps(chat_message))
    except redis.exceptions.ResponseError as e:
        print(f"Caught an error: {e}")

# Example usage
update_player_location('NOOR', '888', '777')
update_player_location('ORCHID', '999', '666')
update_player_location('SAMY', '111', '222')
log_game_event('NOOR', 'kill', 'killed a person')
log_game_event('ORCHID', 'explosion', 'threw a grenade')
log_game_event('SAMY', 'shoot', 'shot a person')
update_leaderboard('NOOR', 800)
update_leaderboard('ORCHID', 700)
update_leaderboard('SAMY', 600)
send_chat_message('guild1', 'NOOR', 'Hello team!')
send_chat_message('guild2', 'ORCHID', 'Send Help')
send_chat_message('guild3', 'SAMY', 'Follow me!')