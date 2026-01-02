import streamlit as st
import pandas as pd
import pydeck as pdk
import time

# 1. Setup & Configuration
st.set_page_config(layout="wide", page_title="SwiftStock: AI Ops Platform", initial_sidebar_state="collapsed")

# 2. Hybrid Connection (Local + Snowflake)
def create_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        from snowflake.snowpark import Session
        if "snowflake" in st.secrets:
            return Session.builder.configs(st.secrets["snowflake"]).create()
        else:
            st.error("Missing .streamlit/secrets.toml")
            st.stop()

session = create_session()

# 3. Data Loading & Logic
@st.cache_data
def load_data():
    # Load Main Inventory (Restored CRITICALITY_LEVEL)
    query_inv = """
    SELECT 
        f.FACILITY_ID, f.FACILITY_NAME, f.LATITUDE, f.LONGITUDE,
        i.ITEM_NAME, i.CLOSING_STOCK, i.PREDICTED_STOCKOUT_DAYS, i.AVG_DAILY_USAGE, i.CRITICALITY_LEVEL,
        CASE 
            WHEN i.CLOSING_STOCK = 0 THEN 'CRITICAL'
            WHEN i.PREDICTED_STOCKOUT_DAYS < 7 THEN 'WARNING'
            ELSE 'HEALTHY'
        END AS STATUS
    FROM FACILITIES f
    JOIN INVENTORY_DAILY_PREDICTED i ON f.FACILITY_ID = i.FACILITY_ID
    """
    df_inv = session.sql(query_inv).to_pandas()
    
    # Load Reorder List
    query_reorder = "SELECT * FROM REORDER_ALERTS ORDER BY PREDICTED_STOCKOUT_DAYS ASC"
    df_reorder = session.sql(query_reorder).to_pandas()
    
    return df_inv, df_reorder

def find_nearest_neighbors(victim_id, item_name):
    """
    Robust version: Uses parameters to prevent SQL injection/errors 
    and constructs GeoPoints on the fly to avoid 'Invalid Identifier' errors.
    """
    # We construct the point manually using LONGITUDE/LATITUDE to be safe
    # We also use '?' for parameters instead of f-strings
    query = """
    SELECT 
        neighbor.FACILITY_NAME as SAVIOR_NAME,
        neighbor.LATITUDE as SAVIOR_LAT,
        neighbor.LONGITUDE as SAVIOR_LON,
        inv.CLOSING_STOCK as AVAILABLE_STOCK,
        ROUND(
            ST_DISTANCE(
                ST_MAKEPOINT(source.LONGITUDE, source.LATITUDE), 
                ST_MAKEPOINT(neighbor.LONGITUDE, neighbor.LATITUDE)
            ) / 1000, 1
        ) as DISTANCE_KM
    FROM FACILITIES source
    JOIN FACILITIES neighbor 
        ON source.FACILITY_ID != neighbor.FACILITY_ID
    JOIN INVENTORY_DAILY_PREDICTED inv 
        ON neighbor.FACILITY_ID = inv.FACILITY_ID
    WHERE source.FACILITY_ID = ?
      AND inv.ITEM_NAME = ?
      AND inv.CLOSING_STOCK > 100 
    ORDER BY DISTANCE_KM ASC
    LIMIT 3;
    """
    
    # Run query with safe parameters
    try:
        return session.sql(query, params=[victim_id, item_name]).to_pandas()
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame() # Return empty DF to prevent crash

def run_chat_action(user_query, valid_items):
    """
    Identifies the medical item from the user's query.
    """
    user_query = user_query.lower()
    # Sort by length to match "Amoxicillin 500mg" before "Amoxicillin"
    valid_items = sorted(valid_items, key=len, reverse=True)
    
    for item in valid_items:
        simple_name = item.split(" ")[0].lower() # e.g. "oxytocin"
        # Check full name or simple name
        if item.lower() in user_query or simple_name in user_query:
            return item
    return None

try:
    df, df_reorder = load_data()
except Exception as e:
    st.error(f"Data Error: {e}")
    st.stop()

# 4. UI Layout
st.title("🏥 SwiftStock: AI Operations Platform")

tab1, tab2, tab3 = st.tabs(["🗺️ Command Center", "📦 Procurement Planning", "🤖 AI Assistant"])

# ==========================================
# TAB 1: COMMAND CENTER (Restored Original Flow)
# ==========================================
with tab1:
    # A. FILTERS (Criticality -> Smart Item Selector)
    col_filter_crit, col_filter_item, col_kpis = st.columns([1, 2, 2])
    
    with col_filter_crit:
        st.markdown("### 1. Priority")
        # Sort levels (High first)
        crit_levels = sorted(df['CRITICALITY_LEVEL'].unique(), key=lambda x: (0 if x=='High' else 1 if x=='Medium' else 2))
        selected_crit = st.selectbox("Criticality Level", crit_levels)
        
    with col_filter_item:
        st.markdown("### 2. Select Item")
        # Filter by criticality first
        df_level = df[df['CRITICALITY_LEVEL'] == selected_crit]
        
        # Calculate status counts for sorting
        item_stats = df_level.groupby('ITEM_NAME')['STATUS'].apply(lambda x: (x == 'CRITICAL').sum()).reset_index()
        item_stats.columns = ['ITEM_NAME', 'CRITICAL_COUNT']
        
        # Formatting: "🔴 Oxytocin (3 Critical)"
        def make_name(row):
            if row['CRITICAL_COUNT'] > 0: return f"🔴 {row['ITEM_NAME']} ({row['CRITICAL_COUNT']} Sites)"
            return f"✅ {row['ITEM_NAME']} (Healthy)"
        
        item_stats['DISPLAY'] = item_stats.apply(make_name, axis=1)
        item_stats = item_stats.sort_values('CRITICAL_COUNT', ascending=False)
        
        selected_display = st.selectbox("Choose Medicine", item_stats['DISPLAY'])
        actual_item = selected_display.split(" (")[0][2:] # Extract name
        
    # Filter Main Data for the Console
    df_filtered = df[df['ITEM_NAME'] == actual_item]
    critical_hospitals = df_filtered[df_filtered['STATUS'] == 'CRITICAL']

    # B. ITEM-SPECIFIC KPIS (Relevant to the Task)
    with col_kpis:
        c1, c2 = st.columns(2)
        c1.metric("Critical Stock-outs", f"{len(critical_hospitals)}", delta_color="inverse")
        c2.metric("Healthy Surplus Sites", f"{len(df_filtered[df_filtered['STATUS']=='HEALTHY'])}")
        
    st.divider()

    # C. EMERGENCY RESPONSE & MAP
    col_action, col_map = st.columns([1, 2])
    map_layers = []
    view_state = pdk.ViewState(latitude=-6.9, longitude=107.6, zoom=8, pitch=0)

    with col_action:
        st.subheader("3. Emergency Console")
        if len(critical_hospitals) > 0:
            st.warning(f"⚠️ {len(critical_hospitals)} hospitals need {actual_item}!")
            
            selected_victim = st.selectbox(
                "Select Site to Resolve:", 
                critical_hospitals['FACILITY_NAME'].unique(),
                index=None,
                placeholder="Choose hospital..."
            )
            
            if selected_victim:
                victim_record = critical_hospitals[critical_hospitals['FACILITY_NAME'] == selected_victim].iloc[0]
                neighbors = find_nearest_neighbors(victim_record['FACILITY_ID'], actual_item)
                
                if not neighbors.empty:
                    st.success(f"✅ Found {len(neighbors)} Options")
                    st.dataframe(neighbors[['SAVIOR_NAME', 'DISTANCE_KM', 'AVAILABLE_STOCK']], hide_index=True)
                    
                    # Add Arc
                    arcs_data = neighbors.copy()
                    arcs_data['source_lon'] = victim_record['LONGITUDE']
                    arcs_data['source_lat'] = victim_record['LATITUDE']
                    
                    layer_arc = pdk.Layer(
                        "ArcLayer",
                        data=arcs_data,
                        get_source_position='[source_lon, source_lat]',
                        get_target_position='[SAVIOR_LON, SAVIOR_LAT]',
                        get_source_color=[200, 30, 0, 160],
                        get_target_color=[0, 200, 30, 160],
                        get_width=5, get_tilt=15
                    )
                    map_layers.append(layer_arc)
                    
                    view_state = pdk.ViewState(latitude=victim_record['LATITUDE'], longitude=victim_record['LONGITUDE'], zoom=10.5, pitch=45)
                else:
                    st.error("No nearby stock found.")
        else:
            st.info("✅ No critical alerts for this item.")

    with col_map:
        def get_color(status):
            if status == 'CRITICAL': return [200, 30, 0, 200]
            if status == 'WARNING': return [255, 140, 0, 160]
            return [0, 200, 30, 100]

        df_filtered['color'] = df_filtered['STATUS'].apply(get_color)
        
        layer_scatter = pdk.Layer(
            "ScatterplotLayer",
            data=df_filtered,
            get_position='[LONGITUDE, LATITUDE]',
            get_color='color',
            get_radius=2000,
            pickable=True,
            filled=True
        )
        map_layers.insert(0, layer_scatter)
        
        st.pydeck_chart(pdk.Deck(
            map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            initial_view_state=view_state,
            layers=map_layers,
            tooltip={"text": "{FACILITY_NAME}\nStock: {CLOSING_STOCK}\nStatus: {STATUS}"}
        ))

# ==========================================
# TAB 2: PROCUREMENT PLANNING (Network KPIs Here)
# ==========================================
with tab2:
    # A. NETWORK HEALTH KPIS (Moved from Tab 1)
    st.markdown("### 📊 Network Health Overview")
    k1, k2, k3 = st.columns(3)
    
    # Calculate Network Metrics
    total_stockouts = len(df[df['CLOSING_STOCK'] == 0])
    predicted_risk = len(df[(df['PREDICTED_STOCKOUT_DAYS'] < 7) & (df['CLOSING_STOCK'] > 0)])
    
    k1.metric("Active Stock-outs (Network)", f"{total_stockouts}", delta_color="inverse")
    k2.metric("Predicted Risks (<7 Days)", f"{predicted_risk}", delta="-Warning", delta_color="inverse")
    k3.metric("Total Network Inventory", f"{df['CLOSING_STOCK'].sum():,}")
    
    st.divider()

    # B. REORDER LIST
    st.header("📋 Automated Reorder List")
    col_info, col_export = st.columns([3, 1])
    
    with col_info:
        st.info("💡 Logic: If Predicted Stockout < 14 Days, order enough for 30 Days coverage.")
    with col_export:
        csv = df_reorder.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download CSV", csv, "procurement_plan.csv", "text/csv")

    st.dataframe(
        df_reorder,
        column_config={
            "PREDICTED_STOCKOUT_DAYS": st.column_config.NumberColumn("Days Left", help="Days until 0 stock"),
            "SUGGESTED_REORDER_QTY": st.column_config.NumberColumn("Order Qty", format="%.0f"),
            "STATUS": st.column_config.TextColumn("Priority"),
        },
        use_container_width=True, hide_index=True
    )

# ==========================================
# TAB 3: AI ASSISTANT (Context-Aware)
# ==========================================
with tab3:
    st.subheader("🧠 SwiftBot: Network Intelligence")
    st.caption("Ask about any item to get a full Ops & Procurement report. Try: *'Status of Oxytocin'*")

    chat_container = st.container()
    
    if prompt := st.chat_input("Ask SwiftBot..."):
        # 1. User Message
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # 2. Identify Item
        valid_items_list = df['ITEM_NAME'].unique().tolist()
        extracted_item = run_chat_action(prompt, valid_items_list)
        
        if extracted_item:
            # --- GATHER INTELLIGENCE (The "Brain") ---
            
            # A. Network Status (Tab 1 Data)
            # How many sites are critical?
            crit_sites = df[(df['ITEM_NAME'] == extracted_item) & (df['STATUS'] == 'CRITICAL')]
            total_stock = df[df['ITEM_NAME'] == extracted_item]['CLOSING_STOCK'].sum()
            
            # B. Procurement Status (Tab 2 Data)
            # Is it on the reorder list?
            reorder_info = df_reorder[df_reorder['ITEM_NAME'] == extracted_item]
            is_urgent = False
            procurement_msg = "✅ Procurement Status: Healthy. No immediate reorder needed."
            
            if not reorder_info.empty:
                rec = reorder_info.iloc[0]
                days_left = rec['PREDICTED_STOCKOUT_DAYS']
                qty = rec['SUGGESTED_REORDER_QTY']
                
                if days_left < 7:
                    is_urgent = True
                    procurement_msg = f"🚨 **PROCUREMENT ALERT:**\n- **Predicted Stockout:** {days_left:.0f} Days\n- **Recommended Order:** {qty:,.0f} units immediately."
                else:
                    procurement_msg = f"⚠️ **Watchlist:** Predicted stockout in {days_left:.0f} days."

            # C. Tactical Solution (Finding Neighbors)
            # Find a 'victim' to simulate the search
            target_id = crit_sites.iloc[0]['FACILITY_ID'] if not crit_sites.empty else df['FACILITY_ID'].iloc[0]
            matches = find_nearest_neighbors(target_id, extracted_item)
            
            if not matches.empty:
                top = matches.iloc[0]
                tactical_msg = f"🚚 **Logistics Solution:**\nNearest surplus found at **{top['SAVIOR_NAME']}** ({top['DISTANCE_KM']} km away)."
            else:
                tactical_msg = "❌ **Logistics Alert:** No nearby surplus found for lateral transfer."

            # --- CONSTRUCT THE RESPONSE ---
            header = f"### 🔎 Analysis for: {extracted_item}"
            
            # Dynamic Opening based on urgency
            if not crit_sites.empty:
                status_icon = "🔴"
                status_text = f"**CRITICAL:** Active stock-outs at {len(crit_sites)} locations."
            elif is_urgent:
                status_icon = "🟠"
                status_text = "**WARNING:** Network-wide supply levels are dropping."
            else:
                status_icon = "🟢"
                status_text = "**HEALTHY:** Supply chain is stable."

            # Combine it all
            resp = f"""
{header}
{status_icon} {status_text}

---
{procurement_msg}

---
{tactical_msg}
            """
            
        else:
            resp = "❓ I couldn't identify a specific medical item in your query. Please mention a drug name (e.g., 'Amoxicillin', 'Oxytocin')."

        # 3. Append Response
        st.session_state.messages.append({"role": "assistant", "content": resp})

    # Render History
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "I am connected to Command Center and Procurement feeds. What item should I check?"}]
    
    with chat_container:
        for msg in st.session_state.messages:
            st.chat_message(msg["role"]).write(msg["content"])