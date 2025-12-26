import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session

# 1. Setup & Configuration
st.set_page_config(
    layout="wide", 
    page_title="SwiftStock Command Center",
    initial_sidebar_state="collapsed"
)
session = get_active_session()

# 2. Data Loading Functions
@st.cache_data
def load_network_status():
    """Load the overall health of the network for the heatmap."""
    query = """
    SELECT 
        f.FACILITY_ID,
        f.FACILITY_NAME,
        f.LATITUDE,
        f.LONGITUDE,
        i.ITEM_NAME,
        i.CLOSING_STOCK,
        i.CRITICALITY_LEVEL, -- Added this for the new filter
        CASE 
            WHEN i.CLOSING_STOCK = 0 THEN 'CRITICAL'
            WHEN i.CLOSING_STOCK < 50 THEN 'LOW'
            ELSE 'HEALTHY'
        END AS STATUS
    FROM FACILITIES f
    JOIN INVENTORY_DAILY i ON f.FACILITY_ID = i.FACILITY_ID
    WHERE i.DATE = CURRENT_DATE() 
       OR i.DATE = (SELECT MAX(DATE) FROM INVENTORY_DAILY)
    """
    return session.sql(query).to_pandas()

def find_nearest_neighbors(victim_id, item_name):
    query = f"""
    SELECT 
        neighbor.FACILITY_NAME as SAVIOR_NAME,
        neighbor.LATITUDE as SAVIOR_LAT,
        neighbor.LONGITUDE as SAVIOR_LON,
        inv.CLOSING_STOCK as AVAILABLE_STOCK,
        ROUND(ST_DISTANCE(source.GEO_POINT, neighbor.GEO_POINT) / 1000, 1) as DISTANCE_KM
    FROM FACILITIES source
    JOIN FACILITIES neighbor 
        ON source.FACILITY_ID != neighbor.FACILITY_ID
    JOIN INVENTORY_DAILY inv 
        ON neighbor.FACILITY_ID = inv.FACILITY_ID
    WHERE source.FACILITY_ID = '{victim_id}'
      AND inv.ITEM_NAME = '{item_name}'
      AND inv.CLOSING_STOCK > 100 
    HAVING DISTANCE_KM < 10
    ORDER BY DISTANCE_KM ASC
    LIMIT 3;
    """
    return session.sql(query).to_pandas()

def run_chat_action(user_query):
    """
    Uses Cortex AI via direct SQL to extract the item name.
    """
    valid_items = str(df['ITEM_NAME'].unique().tolist())
    
    # 1. Construct Prompt
    prompt = f"""
    You are a medical logistics assistant. 
    Map the user's query to one of these valid item names: {valid_items}.
    
    User Query: "{user_query}"
    
    Return ONLY a JSON object with this format: {{"item": "Exact Item Name"}}
    If the item is not found, return {{"item": null}}.
    """
    
    # 2. Run via SQL (Bypasses local python dependency issues)
    # We use parameter binding (?) to handle quotes safely
    try:
        cmd = "SELECT snowflake.cortex.complete(?, ?)"
        
        # Execute and fetch result
        # args: [model_name, prompt_text]
        result = session.sql(cmd, params=["llama3.1-70b", prompt]).collect()
        
        # 3. Parse Response
        response_text = result[0][0] # Get the string out of the Row object
        
        import json
        # Clean potential markdown formatting
        response_clean = response_text.replace("```json", "").replace("```", "").strip()
        data = json.loads(response_clean)
        
        return data.get("item")
        
    except Exception as e:
        st.write(f"Debug Error: {e}") # Uncomment if you need to debug
        return None
    
# Load Data
try:
    df = load_network_status()
except Exception as e:
    st.error(f"Connection Error: {e}")
    st.stop()

# 3. TOP CONTROL PANEL (Updated with Smart Filtering)
st.title("🏥 SwiftStock: Intelligent Logistics")

with st.container():
    # Split into 3 columns for better layout
    col_crit_filter, col_item_filter, col_kpis = st.columns([1, 2, 2])
    
    # --- FILTER 1: CRITICALITY LEVEL ---
    with col_crit_filter:
        st.markdown("### 1. Filter Priority")
        # Get unique levels but force 'High' to be first if it exists
        crit_levels = sorted(df['CRITICALITY_LEVEL'].unique(), key=lambda x: (0 if x=='High' else 1 if x=='Medium' else 2))
        selected_criticality = st.selectbox("Criticality Level", crit_levels)
    
    # --- FILTER 2: SMART ITEM SELECTOR ---
    with col_item_filter:
        st.markdown("### 2. Select Item")
        
        # A. Filter dataframe by Criticality first
        df_level = df[df['CRITICALITY_LEVEL'] == selected_criticality]
        
        # B. Calculate 'Urgency' for each item in this level
        item_stats = df_level.groupby('ITEM_NAME')['STATUS'].apply(lambda x: (x == 'CRITICAL').sum()).reset_index()
        item_stats.columns = ['ITEM_NAME', 'CRITICAL_COUNT']
        
        # C. Create a "Display Name" that shows status (e.g., "🔴 Oxytocin (3 Critical)")
        def make_display_name(row):
            if row['CRITICAL_COUNT'] > 0:
                return f"🔴 {row['ITEM_NAME']} ({row['CRITICAL_COUNT']} Critical)"
            else:
                return f"✅ {row['ITEM_NAME']} (Healthy)"
        
        item_stats['DISPLAY_NAME'] = item_stats.apply(make_display_name, axis=1)
        
        # D. Sort so Red items are ALWAYS at the top
        item_stats = item_stats.sort_values('CRITICAL_COUNT', ascending=False)
        
        # E. The Dropdown
        selected_display = st.selectbox(
            "Select Item", 
            item_stats['DISPLAY_NAME']
        )
        
        # F. Extract the actual Item Name back from the display string
        # Logic: We split by " (" and take the first part, removing the emoji
        # Example: "🔴 Oxytocin (3...)" -> "Oxytocin"
        actual_item_name = selected_display.split(" (")[0][2:] 

    # Filter Main Data based on final item selection
    df_filtered = df[df['ITEM_NAME'] == actual_item_name]
    critical_hospitals = df_filtered[df_filtered['STATUS'] == 'CRITICAL']

    # --- KPIS ---
    with col_kpis:
        c1, c2 = st.columns(2)
        c1.metric("Critical Stock-outs", f"{len(critical_hospitals)}", delta_color="inverse")
        c2.metric("Healthy Surplus", f"{len(df_filtered[df_filtered['STATUS']=='HEALTHY'])}")

st.divider()

# 4. EMERGENCY RESPONSE CONSOLE
st.subheader("3. Emergency Response Console")
col_action, col_map = st.columns([1, 2])

map_layers = []
# Default View: West Java
view_state = pdk.ViewState(latitude=-6.9, longitude=107.6, zoom=8.5, pitch=0) 

with col_action:
    if len(critical_hospitals) > 0:
        st.warning(f"⚠️ Action Required: {len(critical_hospitals)} hospitals have 0 units of {actual_item_name}.")
        
        # THE FIX: Default index=None ensures it starts blank
        selected_victim_name = st.selectbox(
            "Select a Critical Hospital to Resolve:", 
            critical_hospitals['FACILITY_NAME'].unique(),
            index=None,  
            placeholder="Select a hospital to find stock..."
        )
        
        # Only run logic IF a user has actually selected something
        if selected_victim_name:
            victim_record = critical_hospitals[critical_hospitals['FACILITY_NAME'] == selected_victim_name].iloc[0]
            victim_id = victim_record['FACILITY_ID']
            
            neighbors_df = find_nearest_neighbors(victim_id, actual_item_name)
            
            if not neighbors_df.empty:
                st.success(f"✅ Solution Found: {len(neighbors_df)} nearby options.")
                st.dataframe(
                    neighbors_df[['SAVIOR_NAME', 'DISTANCE_KM', 'AVAILABLE_STOCK']], 
                    use_container_width=True, hide_index=True
                )
                
                # Create Arc Layer
                arcs_data = neighbors_df.copy()
                arcs_data['source_lon'] = victim_record['LONGITUDE']
                arcs_data['source_lat'] = victim_record['LATITUDE']
                
                arc_layer = pdk.Layer(
                    "ArcLayer",
                    data=arcs_data,
                    get_source_position='[source_lon, source_lat]',
                    get_target_position='[SAVIOR_LON, SAVIOR_LAT]',
                    get_source_color=[200, 30, 0, 160],
                    get_target_color=[0, 200, 30, 160],
                    get_width=5,
                    get_tilt=15,
                )
                map_layers.append(arc_layer)
                
                # Zoom in on Action
                view_state = pdk.ViewState(
                    latitude=victim_record['LATITUDE'], 
                    longitude=victim_record['LONGITUDE'], 
                    zoom=10.5, 
                    pitch=45
                )
            else:
                st.error("No neighbors found within range.")
    else:
        st.info("✅ All systems healthy. No critical actions required.")

# 5. THE MAP (Fixed Style)
def get_color(status):
    if status == 'CRITICAL': return [200, 30, 0, 200]
    if status == 'LOW': return [255, 140, 0, 160]
    return [0, 200, 30, 100]

df_filtered['color'] = df_filtered['STATUS'].apply(get_color)

scatter_layer = pdk.Layer(
    "ScatterplotLayer",
    data=df_filtered,
    get_position='[LONGITUDE, LATITUDE]',
    get_color='color',
    get_radius=1500,
    pickable=True,
    filled=True,
    radius_min_pixels=5,
    radius_max_pixels=15,
)
map_layers.insert(0, scatter_layer)

with col_map:
    # THE FIX: Using a public CARTO style (no API key needed)
    st.pydeck_chart(pdk.Deck(
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        initial_view_state=view_state,
        layers=map_layers,
        tooltip={"text": "{FACILITY_NAME}\nStock: {CLOSING_STOCK}\nStatus: {STATUS}"}
    ))

# ==========================================
# 5. SWIFTBOT AI ASSISTANT (Sidebar)
# ==========================================
with st.sidebar:
    st.header("🤖 SwiftBot Assistant")
    st.caption("Ask questions like: *'Where can I get Oxygen?'* or *'Find nearest Amoxicillin'*")

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "I can help you find nearby stock instantly. What do you need?"}]

    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat Input
    if prompt := st.chat_input("Type your request..."):
        # 1. User Message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # 2. AI Processing
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            message_placeholder.markdown("🔍 *Scanning network...*")
            
            # Extract Item using Cortex
            extracted_item = run_chat_action(prompt)
            
            if extracted_item:
                # 3. Trigger the "Lateral Transfer" Logic automatically
                # We find the 'closest' victim hospital (just for demo purposes, we pick the first one with 0 stock)
                # In a real app, you'd ask "For which hospital?" - but for speed, we assume the user is the 'current' hospital
                
                matches = find_nearest_neighbors(victim_id, extracted_item) # Uses the victim_id from your main dropdown
                
                if not matches.empty:
                    top_match = matches.iloc[0]
                    response_text = f"**Found it!** \n\n{extracted_item} is available at **{top_match['SAVIOR_NAME']}** ({top_match['DISTANCE_KM']} km away).\n\nThey have {top_match['AVAILABLE_STOCK']} units."
                else:
                    response_text = f"I understood you want **{extracted_item}**, but I couldn't find any nearby surplus."
            else:
                response_text = "I'm sorry, I couldn't identify the medical item in your query. Please try the exact name."

            message_placeholder.markdown(response_text)
            st.session_state.messages.append({"role": "assistant", "content": response_text})