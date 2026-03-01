import streamlit as st

from communication.client import create_node, delete_node, update_node
from communication.spark_service import get_nodes_df

NODE_TYPES = ["producer", "consumer", "accumulator"]

st.set_page_config(page_title="Node Management", layout="wide")
st.title("Node Management")

st.subheader("Current nodes")
col_refresh, _ = st.columns([1, 5])
with col_refresh:
    refresh_nodes = st.button("Refresh", key="refresh_nodes")

if refresh_nodes or "nodes_df" not in st.session_state:
    with st.spinner("Reading node-lifecycle-events via Spark..."):
        st.session_state["nodes_df"] = get_nodes_df()

df_nodes = st.session_state.get("nodes_df")
if df_nodes is not None and not df_nodes.empty:
    st.dataframe(df_nodes, use_container_width=True)
else:
    st.info("No nodes found yet.")

st.divider()

tab_create, tab_update, tab_delete = st.tabs(["Create", "Update", "Delete"])

with tab_create:
    st.subheader("Create a new node")
    with st.form("form_create"):
        cr_user = st.text_input("Owner user ID", placeholder="u_1a2b3c4d")
        cr_district = st.text_input("District ID", placeholder="d_1")
        cr_type = st.selectbox("Node type", NODE_TYPES)
        submitted_cr = st.form_submit_button("Create", use_container_width=True)

    if submitted_cr:
        if not cr_user or not cr_district:
            st.error("User ID and District ID are required.")
        else:
            with st.spinner("Waiting for DistrictNodeManager response..."):
                response = create_node(cr_user, cr_district, cr_type)
            if response is None:
                st.error("Timeout: no response received within the deadline.")
            elif not response.get("success", True):
                st.warning(f"Request rejected: {response}")
            else:
                payload = response.get("payload", {})
                st.success(
                    f"Node created — ID: `{payload.get('nodeId')}`  "
                    f"| type: `{payload.get('nodeType')}`  "
                    f"| district: `{payload.get('districtId')}`  "
                    f"| owner: `{payload.get('userId')}`"
                )
            with st.expander("Raw response"):
                st.json(response or {})

with tab_update:
    st.subheader("Update an existing node")
    with st.form("form_update"):
        up_node = st.text_input("Node ID", placeholder="n_2")
        up_user = st.text_input("Owner user ID", placeholder="u_1a2b3c4d")
        up_district = st.text_input("District ID", placeholder="d_1")
        up_type = st.selectbox("Node type", NODE_TYPES, key="up_type")
        submitted_up = st.form_submit_button("Update", use_container_width=True)

    if submitted_up:
        if not up_node:
            st.error("Node ID is required.")
        else:
            with st.spinner("Waiting for DistrictNodeManager response..."):
                response = update_node(up_node, up_user, up_district, up_type)
            if response is None:
                st.error("Timeout: no response received within the deadline.")
            elif not response.get("success", True):
                st.warning(f"Request rejected: {response}")
            else:
                payload = response.get("payload", {})
                st.success(
                    f"Node updated — ID: `{payload.get('nodeId')}`  "
                    f"| type: `{payload.get('nodeType')}`  "
                    f"| district: `{payload.get('districtId')}`"
                )
            with st.expander("Raw response"):
                st.json(response or {})

with tab_delete:
    st.subheader("Delete a node")
    with st.form("form_delete"):
        dl_node = st.text_input("Node ID", placeholder="n_2")
        dl_user = st.text_input("Owner user ID", placeholder="u_1a2b3c4d")
        submitted_dl = st.form_submit_button("Delete", use_container_width=True)

    if submitted_dl:
        if not dl_node:
            st.error("Node ID is required.")
        else:
            with st.spinner("Waiting for DistrictNodeManager response..."):
                response = delete_node(dl_node, dl_user)
            if response is None:
                st.error("Timeout: no response received within the deadline.")
            elif not response.get("success", True):
                st.warning(f"Request rejected: {response}")
            else:
                payload = response.get("payload", {})
                st.success(f"Node deleted — ID: `{payload.get('nodeId')}`")
            with st.expander("Raw response"):
                st.json(response or {})
