"""
Redirect page for the old Streamlit Community Cloud address.

The dashboard itself (app.py) runs on Railway. The Streamlit Cloud app at
lumiere-email-tracker.streamlit.app runs this file instead, so old links and
bookmarks keep working and no dashboard data is ever served from there
(public Streamlit Cloud apps hand search engines a saved copy of the page).

Set DASHBOARD_URL (the Railway address) in the Streamlit Cloud app's secrets.
"""

import json
import os

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Lumiere Email Response Dashboard", page_icon="📧")

st.title("Lumiere Email Response Dashboard")

dashboard_url = os.getenv("DASHBOARD_URL", "").strip()
if not dashboard_url.startswith("https://"):
    st.info("The dashboard has moved. Ask an admin for the new link.")
    st.stop()

st.write("The dashboard has moved. Taking you there...")
st.link_button("Open the dashboard", dashboard_url, type="primary")

# Send the whole browser tab (not just Streamlit's frame) to the new address,
# and keep this page out of search results
components.html(
    f"""
    <script>
    (function () {{
        var url = {json.dumps(dashboard_url)};
        [window.parent, window.top].forEach(function (w) {{
            try {{
                var doc = w.document;
                if (!doc.querySelector('meta[name="robots"]')) {{
                    var meta = doc.createElement("meta");
                    meta.name = "robots";
                    meta.content = "noindex, nofollow";
                    doc.head.appendChild(meta);
                }}
            }} catch (e) {{}}
        }});
        // This frame is sandboxed and can't navigate the tab itself, so run
        // the redirect from Streamlit's page, which can
        var script = window.parent.document.createElement("script");
        script.textContent = "window.top.location.replace(" + JSON.stringify(url) + ");";
        window.parent.document.head.appendChild(script);
    }})();
    </script>
    """,
    height=0,
)
