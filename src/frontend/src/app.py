import streamlit as st

st.set_page_config(
    page_title="NSDS – Smart Grid",
    layout="wide",
)

pg = st.navigation([
    st.Page("pages/1_User_Management.py", title="User Management"),
    st.Page("pages/2_Node_Management.py", title="Node Management"),
    st.Page("pages/3_Monitoring.py", title="Monitoring"),
])
pg.run()
