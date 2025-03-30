import json
import bz2
import os
import redis
import glob
import time
import zipfile
from datetime import datetime
import streamlit as st
from collections import defaultdict

# Initialize Redis connection
redis_conn = redis.Redis(host='localhost', port=6379, decode_responses=True)

if __name__ == "__main__":
    print(f"Connected to Redis: {redis_conn.ping()}")

    # clear
    redis_conn.flushdb()
    print("Database flushed!")

    # Data paths
    data_dir = 'data'
    zip_path = 'data2/20221231_23.zip'
    files = []

    # Get from Zip
    if os.path.exists(zip_path):
        print(f"Using ZIP file: {zip_path}")
        extract_dir = os.path.join(os.path.dirname(zip_path), "extracted_data")
        os.makedirs(extract_dir, exist_ok=True)

        # get from bz2
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            print(f"ZIP contents: {zip_ref.namelist()}")
            for file in zip_ref.namelist():
                if file.endswith('.json.bz2'):
                    zip_ref.extract(file, extract_dir)
                    files.append(os.path.join(extract_dir, file))

    # data directory else
    if not files:
        print(f"No files found in ZIP or ZIP not found. Falling back to data directory.")
        files = glob.glob(os.path.join(data_dir, '*.json.bz2'))
        print(f"Found {len(files)} files in data directory.")

    # start with 10 files
    files.sort()
    files = files[:10]  # Limit to 10 files

    count = 0
    processed_files = 0

    total_users = set()
    user_chirp_count = defaultdict(int)

    for file_path in files:
        print(f"Processing {file_path}...")

        try:
            with bz2.open(file_path, 'rt', encoding='utf-8', errors='replace') as file:
                for line_num, line in enumerate(file, 1):
                    if not line.strip():
                        continue

                    try:
                        tweet = json.loads(line)

                        # Keeping english tweets
                        if tweet.get('lang') != 'en':
                            continue

                        # We extract user information
                        if 'user' in tweet:
                            user_data = tweet['user']
                            user_id = user_data['id_str']
                            screen_name = user_data['screen_name']

                            # Add to set of all users
                            total_users.add(user_id)

                            # Store user in Redis
                            user_key = f"user:{user_id}"
                            redis_conn.hset(user_key, mapping={
                                'id': user_id,
                                'screen_name': screen_name,
                                'name': user_data.get('name', ''),
                                'followers_count': user_data.get('followers_count', 0),
                                'chirps_count': 0,  # Will increment later
                                'created_at': user_data.get('created_at', '')
                            })

                            # Add to set of all users
                            redis_conn.sadd('users', user_id)

                            # Add to sorted set by followers
                            redis_conn.zadd('top_users_by_followers',
                                            {user_id: int(user_data.get('followers_count', 0))})

                            # Create mapping for screen_name to user_id
                            redis_conn.set(f"screen_name:{screen_name}", user_id)

                        # Extract chirp info
                        chirp_id = tweet['id_str']
                        user_id = tweet['user']['id_str']
                        created_at = tweet['created_at']
                        text = tweet['text']

                        # Timestamp conversion
                        try:
                            timestamp = int(time.mktime(datetime.strptime(created_at,
                                                                          "%a %b %d %H:%M:%S +0000 %Y").timetuple()))
                        except:
                            timestamp = int(time.time())

                        # Storing into redis
                        chirp_key = f"chirp:{chirp_id}"
                        redis_conn.hset(chirp_key, mapping={
                            'id': chirp_id,
                            'user_id': user_id,
                            'text': text,
                            'created_at': created_at,
                            'timestamp': timestamp
                        })

                        # Adding the chirps to collections
                        redis_conn.sadd('chirps', chirp_id)
                        redis_conn.sadd(f"user:{user_id}:chirps", chirp_id)
                        redis_conn.zadd('chirps_by_time', {chirp_id: timestamp})
                        redis_conn.lpush('latest_chirps', chirp_id)
                        redis_conn.ltrim('latest_chirps', 0, 4)  # Keep only latest 5

                        # Increment user's chirp count
                        redis_conn.hincrby(f"user:{user_id}", 'chirps_count', 1)
                        user_chirp_count[user_id] += 1

                        count += 1
                        if count % 100 == 0:
                            print(f"Processed {count} tweets...")

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON at line {line_num}: {str(e)[:50]}...")
                        continue
                    except Exception as e:
                        print(f"Error processing tweet at line {line_num}: {str(e)[:50]}...")
                        continue

            processed_files += 1
            print(f"Finished processing {file_path}")

        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")

    # Update sorted set for users ( here by chirp count )
    for user_id, count in user_chirp_count.items():
        redis_conn.zadd('top_users_by_chirps', {user_id: count})

    # print data
    total_users_count = redis_conn.scard('users')
    total_chirps_count = redis_conn.scard('chirps')
    print(f"\nDatabase Statistics:")
    print(f"Total Users: {total_users_count}")
    print(f"Total Chirps: {total_chirps_count}")

    # Task: top users by followers
    print("\nTop 5 Users by Followers:")
    top_followers = redis_conn.zrevrange('top_users_by_followers', 0, 4, withscores=True)
    for i, (user_id, score) in enumerate(top_followers, 1):
        user = redis_conn.hgetall(f"user:{user_id}")
        print(f"{i}. {user.get('screen_name', 'Unknown')} - {int(score)} followers")

    # Task: top users by chirps
    print("\nTop 5 Users by Chirps:")
    top_chirps = redis_conn.zrevrange('top_users_by_chirps', 0, 4, withscores=True)
    for i, (user_id, score) in enumerate(top_chirps, 1):
        user = redis_conn.hgetall(f"user:{user_id}")
        print(f"{i}. {user.get('screen_name', 'Unknown')} - {int(score)} chirps")

    # Task: latest chirps
    print("\nLatest 5 Chirps:")
    latest_chirps = redis_conn.lrange('latest_chirps', 0, 4)
    for i, chirp_id in enumerate(latest_chirps, 1):
        chirp = redis_conn.hgetall(f"chirp:{chirp_id}")
        user_id = chirp.get('user_id', '')
        user = redis_conn.hgetall(f"user:{user_id}")
        print(f"{i}. @{user.get('screen_name', 'Unknown')}: {chirp.get('text', '')[:50]}...")