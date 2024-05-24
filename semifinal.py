from cassandra.cluster import Cluster
import redis
import json
from uuid import uuid4
from datetime import datetime

# Connect to Cassandra running in Docker
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()


# # Setup keyspace
# session.execute("""
# CREATE KEYSPACE IF NOT EXISTS gaming
# WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
# """)

# # Setup tables in Cassandra
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
update_player_location('noor', '-999', '55555')
update_player_location('KOKKKKY', '22222', '9999999')
log_game_event('koky', 'item_pickup', 'picked up a rare sword')
update_leaderboard('player1', 500)
send_chat_message('guild1', 'player1', 'Hello team!')
