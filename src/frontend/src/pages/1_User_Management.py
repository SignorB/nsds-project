import streamlit as st

from communication.client import register_user, remove_user, update_user
from communication.spark_service import get_users_df

st.set_page_config(page_title="User Management", layout="wide")
st.title("User Management")

st.subheader("Current users")
col_refresh, _ = st.columns([1, 5])
with col_refresh:
    refresh_users = st.button("Refresh", key="refresh_users")

if refresh_users or "users_df" not in st.session_state:
    with st.spinner("Reading user-lifecycle-events via Spark..."):
        st.session_state["users_df"] = get_users_df()

df_users = st.session_state.get("users_df")
if df_users is not None and not df_users.empty:
    st.dataframe(df_users, use_container_width=True)
else:
    st.info("No users found yet.")

st.divider()

tab_register, tab_update, tab_remove = st.tabs(["Register", "Update", "Remove"])

with tab_register:
    st.subheader("Register a new user")
    with st.form("form_register"):
        reg_email = st.text_input("E-mail", placeholder="user@example.com")
        reg_name = st.text_input("Full name", placeholder="Jane Doe")
        submitted = st.form_submit_button("Register", use_container_width=True)

    if submitted:
        if not reg_email or not reg_name:
            st.error("Both e-mail and full name are required.")
        else:
            with st.spinner("Waiting for AccountService response..."):
                response = register_user(reg_email, reg_name)
            if response is None:
                st.error("Timeout: no response received within the deadline.")
            elif not response.get("success", True):
                st.warning(f"Request rejected: {response}")
            else:
                payload = response.get("payload", {})
                st.success(
                    f"User registered — ID: `{payload.get('userId')}`  "
                    f"| e-mail: `{payload.get('email')}`  "
                    f"| name: `{payload.get('fullName')}`"
                )
            with st.expander("Raw response"):
                st.json(response or {})

with tab_update:
    st.subheader("Update an existing user")
    with st.form("form_update"):
        upd_uid = st.text_input("User ID", placeholder="u_1a2b3c4d")
        upd_email = st.text_input("New e-mail", placeholder="newemail@example.com")
        upd_name = st.text_input("New full name", placeholder="Jane Smith")
        submitted_upd = st.form_submit_button("Update", use_container_width=True)

    if submitted_upd:
        if not upd_uid:
            st.error("User ID is required.")
        else:
            with st.spinner("Waiting for AccountService response..."):
                response = update_user(upd_uid, upd_email, upd_name)
            if response is None:
                st.error("Timeout: no response received within the deadline.")
            elif not response.get("success", True):
                st.warning(f"Request rejected: {response}")
            else:
                payload = response.get("payload", {})
                st.success(
                    f"User updated — ID: `{payload.get('userId')}`  "
                    f"| e-mail: `{payload.get('email')}`  "
                    f"| name: `{payload.get('fullName')}`"
                )
            with st.expander("Raw response"):
                st.json(response or {})

with tab_remove:
    st.subheader("Remove a user")
    with st.form("form_remove"):
        rem_uid = st.text_input("User ID", placeholder="u_1a2b3c4d")
        submitted_rem = st.form_submit_button("Remove", use_container_width=True)

    if submitted_rem:
        if not rem_uid:
            st.error("User ID is required.")
        else:
            with st.spinner("Waiting for AccountService response..."):
                response = remove_user(rem_uid)
            if response is None:
                st.error("Timeout: no response received within the deadline.")
            elif not response.get("success", True):
                st.warning(f"Request rejected: {response}")
            else:
                payload = response.get("payload", {})
                st.success(f"User removed — ID: `{payload.get('userId')}`")
            with st.expander("Raw response"):
                st.json(response or {})
