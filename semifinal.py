import uuid
import redis
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider


# Initialize Redis and Cassandra connections
# try:
#     r = redis.Redis(host='localhost', port=9042, db=0, decode_responses=True)
#     cluster = Cluster(['localhost'])
#     session = cluster.connect('GamePlatform')
#     session.default_consistency_level = ConsistencyLevel.QUORUM
# except Exception as e:
#     print(f"Failed to connect to databases: {e}")
#     exit(1)

# def connect_to_redis(host='localhost', port=6379, db=0):
#     try:
#         r = redis.Redis(host=host, port=port, db=db)
#         # Simple check to see if the server is accessible
#         r.ping()
#         print("Connected to Redis")
#         return r
#     except Exception as e:
#         print(f"Failed to connect to Redis: {e}")
#         return None
    


# def connect_to_cassandra(hosts=['localhost'], keyspace=None):
#     try:
#         # If authentication is needed:
#         # auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')
#         # cluster = Cluster(hosts, auth_provider=auth_provider)
#         cluster = Cluster(hosts)
#         session = cluster.connect(keyspace)
#         print("Connected to Cassandra")
#         return session
#     except Exception as e:
#         print(f"Failed to connect to Cassandra: {e}")
#         return None

# Initialize Cassandra connection
# cassandra_session = connect_to_cassandra(keyspace='GamePlatform')


# # Initialize Redis connection
# redis_conn = connect_to_redis()


# Initialize Redis connection
def connect_to_redis():
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        print("Connected to Redis")
        return r
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return None

# Initialize Cassandra connection
def connect_to_cassandra():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect('game_platform')
        print("Connected to Cassandra")
        return session
    except Exception as e:
        print(f"Failed to connect to Cassandra: {e}")
        return None

redis_conn = connect_to_redis()
cassandra_session = connect_to_cassandra()

# Example function to interact with Redis
def update_redis_data(redis_conn, key, value):
    if redis_conn:
        redis_conn.set(key, value)
        print(f"Data updated in Redis: {key} - {value}")

# Example function to interact with Cassandra
def query_cassandra_data(cassandra_session, query):
    if cassandra_session:
        try:
            result = cassandra_session.execute(query)
            for row in result:
                print(row)
        except Exception as e:
            print(f"Failed to execute query in Cassandra: {e}")

# Use these functions
update_redis_data(redis_conn, 'test_key', 'Hello World')
query_cassandra_data(cassandra_session, "SELECT * FROM some_table")




# Enhanced Cassandra Functions
def manage_player_profile(session, username, email=None, achievements=None, inventory=None, friend_list=None, update=False):
    try:
        if update:
            updates = []
            if email: updates.append(f"email = '{email}'")
            if achievements: updates.append(f"achievements = {achievements}")
            if inventory: updates.append(f"inventory = {inventory}")
            if friend_list: updates.append(f"friend_list = {friend_list}")
            update_query = " SET " + ", ".join(updates) + f" WHERE username = '{username}'"
            query = SimpleStatement(f"UPDATE PlayerProfiles {update_query}", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            session.execute(query)
        else:
            query = SimpleStatement(
                """
                INSERT INTO PlayerProfiles (username, email, achievements, inventory, friend_list)
                VALUES (%s, %s, %s, %s, %s)
                """,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            session.execute(query, (username, email, achievements, inventory, friend_list))
    except Exception as e:
        print(f"Error managing player profile: {e}")

# Error Handling and Validation for Redis Operations
def update_leaderboard(r, player, score):
    try:
        if not isinstance(score, int):
            raise ValueError("Score must be an integer")
        r.zadd('game_leaderboard', {player: score})
        print(f"Updated leaderboard: {player} - {score}")
    except Exception as e:
        print(f"Error updating leaderboard: {e}")


def publish_game_event(r, event_type, details):
    try:
        r.publish('game_events', f"{event_type}: {details}")
        print(f"Published game event '{event_type}: {details}'")
    except Exception as e:
        print(f"Error publishing game event: {e}")

# Input Sanitization
def sanitize_input(input_string):
    return ''.join(char for char in input_string if char.isalnum() or char.isspace())

# Command-line interaction with input validation
def main():
    redis_conn = connect_to_redis()
    if not redis_conn:
        print("Failed to connect to Redis. Exiting...")
        return

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
            email = input("Enter email (optional, press enter to skip): ").strip()
            achievements = input("Enter achievements (optional, press enter to skip, list format): ").strip()
            inventory = input("Enter inventory (optional, press enter to skip, dictionary format): ").strip()
            friend_list = input("Enter friend list (optional, press enter to skip, list format): ").strip()
            update = input("Is this an update? (yes/no): ").lower() == 'yes'
            
            # Attempt to evaluate inputs safely
            try:
                achievements_eval = eval(achievements or "[]")
                inventory_eval = eval(inventory or "{}")
                friend_list_eval = eval(friend_list or "[]")
                manage_player_profile(cassandra_session,username, email if email else None, achievements_eval, inventory_eval, friend_list_eval, update)
                print("Profile processed successfully.")
            except Exception as e:
                print(f"An error occurred while processing the profile: {e}")

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
