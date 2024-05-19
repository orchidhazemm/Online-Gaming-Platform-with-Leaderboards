import uuid
import redis
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

# Initialize Redis and Cassandra connections
try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    cluster = Cluster(['localhost'])
    session = cluster.connect('GamePlatform')
except Exception as e:
    print(f"Failed to connect to databases: {e}")
    exit(1)

# Function to manage player profiles
def manage_player_profile(username, email=None, profile_picture=None, achievements=None, inventory=None, friend_list=None, update=False):
    try:
        if update:
            updates = []
            if email: updates.append(f"email = '{email}'")
            if profile_picture: updates.append(f"profile_picture = {profile_picture}")
            if achievements: updates.append(f"achievements = {achievements}")
            if inventory: updates.append(f"inventory = {inventory}")
            if friend_list: updates.append(f"friend_list = {friend_list}")
            update_query = " SET " + ", ".join(updates) + f" WHERE username = '{username}'"
            query = SimpleStatement(f"UPDATE GamePlatform.PlayerProfiles {update_query}", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        else:
            query = SimpleStatement(
                f"""
                INSERT INTO GamePlatform.PlayerProfiles (username, email, profile_picture, achievements, inventory, friend_list)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            session.execute(query, (username, email, profile_picture, achievements, inventory, friend_list))
    except Exception as e:
        print(f"Error managing player profile: {e}")

# Function to update leaderboard in Redis
def update_leaderboard(player, score):
    try:
        r.zadd('game_leaderboard', {player: score})
    except Exception as e:
        print(f"Error updating leaderboard: {e}")

# Function to publish game event in Redis
def publish_game_event(event_type, details):
    try:
        r.publish('game_events', f"{event_type}: {details}")
    except Exception as e:
        print(f"Error publishing game event: {e}")

# Command-line interface for interaction
def main():
    while True:
        print("\nAvailable Actions:")
        print("1. Add/Update Player Profile")
        print("2. Update Leaderboard")
        print("3. Publish Game Event")
        print("4. Exit")
        choice = input("Enter choice: ")

        if choice == '1':
            username = input("Enter username: ")
            email = input("Enter email (optional, press enter to skip): ")
            profile_picture = input("Enter profile picture (optional, press enter to skip): ")
            achievements = input("Enter achievements (optional, press enter to skip, list format): ")
            inventory = input("Enter inventory (optional, press enter to skip, dictionary format): ")
            friend_list = input("Enter friend list (optional, press enter to skip, list format): ")
            update = input("Is this an update? (yes/no): ").lower() == 'yes'
            manage_player_profile(username, email, profile_picture, eval(achievements or "[]"), eval(inventory or "{}"), eval(friend_list or "[]"), update)
        elif choice == '2':
            player = input("Enter player username: ")
            score = int(input("Enter score: "))
            update_leaderboard(player, score)
        elif choice == '3':
            event_type = input("Enter event type: ")
            details = input("Enter event details: ")
            publish_game_event(event_type, details)
        elif choice == '4':
            print("Exiting...")
            break
        else:
            print("Invalid choice, try again.")

if __name__ == "__main__":
    main()
