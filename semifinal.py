from cassandra.cluster import Cluster
<<<<<<< Updated upstream
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
=======
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

# Initialize Redis connection
def connect_to_redis():
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
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
=======
        cluster = Cluster(['localhost'])
        session = cluster.connect('gameplatform')  # Ensure keyspace is specified here
        print("Connected to Cassandra")
        return session
    except Exception as e:
        print(f"Failed to connect to Cassandra: {e}")
        return None

redis_conn = connect_to_redis()
cassandra_session = connect_to_cassandra()

# Enhanced Cassandra Functions
def manage_player_profile(session, username, email=None, achievements=None, inventory=None, friend_list=None, update=False):
    try:
        keyspace_table = 'gameplatform.player_profiles'  # Include keyspace in table reference
        if update:
            updates = []
            if email: updates.append(f"email = '{email}'")
            if achievements: updates.append(f"achievements = {achievements}")
            if inventory: updates.append(f"inventory = {inventory}")
            if friend_list:
                formatted_friend_list = [str(fid) for fid in friend_list]
                updates.append(f"friend_list = {formatted_friend_list}")
            if updates:
                update_query = " SET " + ", ".join(updates) + f" WHERE username = '{username}'"
                query_str = f"UPDATE {keyspace_table} {update_query}"
                print(f"Executing query: {query_str}")
                query = SimpleStatement(query_str, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
                session.execute(query)
            else:
                print("No fields to update")
        else:
            query_str = f"""
                INSERT INTO {keyspace_table} (player_id, username, email, profile_picture, achievements, inventory, friend_list)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            print(f"Executing query: {query_str} with values ({uuid.uuid4()}, {username}, {email}, 'profilepic.jpg', {achievements}, {inventory}, {friend_list})")
            query = SimpleStatement(query_str, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            session.execute(query, (uuid.uuid4(), username, email, 'profilepic.jpg', achievements, inventory, friend_list))
        print("Query executed successfully")
    except Exception as e:
        print(f"Error managing player profile: {e}")

# Redis Functions
def update_leaderboard(r, player, score):
>>>>>>> Stashed changes
    try:
        redis_client.rpush(f'chat_messages:{guild_id}', json.dumps(chat_message))
    except redis.exceptions.ResponseError as e:
        print(f"Caught an error: {e}")

<<<<<<< Updated upstream
# Example usage
update_player_location('Playerorchidhazem', '2003', '010258')
update_player_location('SucessAcadenmy', '96301', '8794')
log_game_event('koky', 'item_pickup', 'picked up a rare sword')
update_leaderboard('player1', 500)
send_chat_message('guild1', 'player1', 'Hello team!')
=======
def publish_game_event(r, event_type, details):
    try:
        r.publish('game_events', f"{event_type}: {details}")
        print(f"Published game event '{event_type}: {details}'")
    except Exception as e:
        print(f"Error publishing game event: {e}")

# Command-line interaction with input validation
def main():
    predefined_emails = ['playerone@example.com', 'playertwo@example.com', 'playerthree@example.com']
    predefined_achievements = [['First Quest', 'Sharp Shooter'], ['Master Explorer', 'Dungeon Crawler'], ['Legendary Hero', 'Puzzle Solver']]
    predefined_inventory = [{'sword': 1, 'shield': 2}, {'bow': 1, 'arrow': 20}, {'wand': 1, 'cloak': 1}]
    predefined_friend_lists = [[uuid.uuid4(), uuid.uuid4()], [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()], [uuid.uuid4()]]

    while True:
        print("\nAvailable Actions:")
        print("1. Add/Update Player Profile")
        print("2. Update Leaderboard")
        print("3. Publish Game Event")
        print("4. Exit")
        choice = input("Enter choice: ")

        if choice == '1':
            print("Adding/Updating Player Profile...")
            username = input("Enter username: ").strip()

            print("Choose an email:")
            for idx, email in enumerate(predefined_emails, 1):
                print(f"{idx}. {email}")
            email_choice = int(input("Enter choice (number): ").strip())
            email = predefined_emails[email_choice - 1]

            print("Choose achievements:")
            for idx, achievements in enumerate(predefined_achievements, 1):
                print(f"{idx}. {achievements}")
            achievements_choice = int(input("Enter choice (number): ").strip())
            achievements = predefined_achievements[achievements_choice - 1]

            print("Choose inventory:")
            for idx, inventory in enumerate(predefined_inventory, 1):
                print(f"{idx}. {inventory}")
            inventory_choice = int(input("Enter choice (number): ").strip())
            inventory = predefined_inventory[inventory_choice - 1]

            print("Choose friend list:")
            for idx, friend_list in enumerate(predefined_friend_lists, 1):
                print(f"{idx}. {friend_list}")
            friend_list_choice = int(input("Enter choice (number): ").strip())
            friend_list = predefined_friend_lists[friend_list_choice - 1]

            update = input("Is this an update? (yes/no): ").lower() == 'yes'

            manage_player_profile(cassandra_session, username, email, achievements, inventory, friend_list, update)
            print("Profile processed successfully.")

        elif choice == '2':
            print("Updating Leaderboard...")
            player = input("Enter player username: ").strip()
            score = input("Enter score: ").strip()
            try:
                score = int(score)
                update_leaderboard(redis_conn, player, score)
            except ValueError:
                print("Score must be an integer. Please try again.")

        elif choice == '3':
            print("Publishing Game Event...")
            event_type = input("Enter event type: ").strip()
            details = input("Enter event details: ").strip()
            publish_game_event(redis_conn, event_type, details)

        elif choice == '4':
            print("Exiting...")
            break
        else:
            print("Invalid choice, try again.")

if __name__ == "__main__":
    main()
>>>>>>> Stashed changes
