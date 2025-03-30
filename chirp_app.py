import streamlit as st
import redis
import time
from datetime import datetime
import uuid

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
st.title("Chirp - Simple Twitter Clone")
st.write(f"Total Users: {r.scard('users')}")
st.write(f"Total Chirps: {r.scard('chirps')}")

tab1, tab2, tab3 = st.tabs(["Latest Chirps", "Top Users", "Post Chirp"])

# Latest chirps
with tab1:
    st.header("Latest Chirps")
    chirp_ids = r.lrange('latest_chirps', 0, 4)

    if not chirp_ids:
        st.write("No chirps found!")
    else:
        for chirp_id in chirp_ids:
            chirp = r.hgetall(f"chirp:{chirp_id}")
            user_id = chirp.get('user_id', '')
            user = r.hgetall(f"user:{user_id}")

            st.write(f"**@{user.get('screen_name', 'unknown')}**: {chirp.get('text', '')}")

            # Convert the Twitter-style timestamp 
            created_at = chirp.get('created_at', '')
            if created_at:
                try:
                    dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
                    friendly_date = dt.strftime("%b %d, %Y at %I:%M %p")
                    st.write(f"Posted: {friendly_date}")
                except:
                    st.write(f"Posted: {created_at}")
            else:
                st.write("Posted: Unknown time")

            st.write("---")

# top Users
with tab2:
    st.header("Top Users by Followers")
    top_users = r.zrevrange('top_users_by_followers', 0, 4, withscores=True)

    if not top_users:
        st.write("No users found!")
    else:
        for i, (user_id, score) in enumerate(top_users, 1):
            user = r.hgetall(f"user:{user_id}")
            st.write(f"{i}. @{user.get('screen_name', 'unknown')} - {int(score)} followers")

    st.header("Top Users by Chirps")
    top_chirpers = r.zrevrange('top_users_by_chirps', 0, 4, withscores=True)

    if not top_chirpers:
        st.write("No users found!")
    else:
        for i, (user_id, score) in enumerate(top_chirpers, 1):
            user = r.hgetall(f"user:{user_id}")
            st.write(f"{i}. @{user.get('screen_name', 'unknown')} - {int(score)} chirps")

# New Tweet Form
with tab3:
    st.header("Post a new Chirp")

    username = st.text_input("Your username")
    text = st.text_area("What's happening?", max_chars=280)

    if st.button("Post Chirp"):
        if not username or not text:
            st.error("Please enter both username and text!")
        else:
            # Check if user exists, if not create a new one
            user_id = r.get(f"screen_name:{username}")

            if not user_id:
                # Create a new user with a unique ID
                user_id = str(uuid.uuid4())
                user_key = f"user:{user_id}"

                # Store user in Redis
                r.hset(user_key, mapping={
                    'id': user_id,
                    'screen_name': username,
                    'name': username,
                    'followers_count': 0,
                    'chirps_count': 0,
                    'created_at': datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y")
                })

                # Add to set of all users
                r.sadd('users', user_id)

                # Add to sorted set by followers
                r.zadd('top_users_by_followers', {user_id: 0})

                # Create mapping for screen_name to user_id
                r.set(f"screen_name:{username}", user_id)

            # Create a new chirp
            chirp_id = str(uuid.uuid4())
            created_at = datetime.now().strftime("%a %b %d %H:%M:%S +0000 %Y")
            timestamp = int(time.time())

            # Store chirp in Redis
            chirp_key = f"chirp:{chirp_id}"
            r.hset(chirp_key, mapping={
                'id': chirp_id,
                'user_id': user_id,
                'text': text,
                'created_at': created_at,
                'timestamp': timestamp
            })

            # Adding the chirp to collections
            r.sadd('chirps', chirp_id)
            r.sadd(f"user:{user_id}:chirps", chirp_id)
            r.zadd('chirps_by_time', {chirp_id: timestamp})
            r.lpush('latest_chirps', chirp_id)
            r.ltrim('latest_chirps', 0, 4)  # Keep only latest 5

            # Increment user's chirp count
            r.hincrby(f"user:{user_id}", 'chirps_count', 1)

            # Update sorted set for users by chirp count
            current_count = int(r.hget(f"user:{user_id}", 'chirps_count'))
            r.zadd('top_users_by_chirps', {user_id: current_count})

            st.success(f"Chirp posted as @{username}!")
            st.rerun()  # Refresh the app to show the new chirp
