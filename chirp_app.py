import streamlit as st
import redis

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
            st.write(f"Posted: {chirp.get('created_at', '')}")
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
            st.success(f"Chirp posted as @{username}! (not really saved)")
            st.info("Note: This is just a simulation, chirps aren't actually saved.")