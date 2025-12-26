import pandas as pd
import random
import uuid
from datetime import datetime
from geopy.distance import geodesic

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
INPUT_FILE = 'hospitals.csv'
OUTPUT_FILE = 'inventory_data.csv'

# Expanded list of 25 medical items
MASTER_ITEMS = [
    {'name': 'Oxytocin Injection', 'cat': 'Maternal', 'crit': 'High'}, # Demo Item
    {'name': 'Magnesium Sulfate', 'cat': 'Maternal', 'crit': 'High'},
    {'name': 'Amoxicillin 500mg', 'cat': 'Antibiotic', 'crit': 'Medium'},
    {'name': 'Azithromycin', 'cat': 'Antibiotic', 'crit': 'Medium'},
    {'name': 'Insulin Glargine', 'cat': 'Chronic', 'crit': 'High'},
    {'name': 'Metformin 500mg', 'cat': 'Chronic', 'crit': 'Medium'},
    {'name': 'Amlodipine 5mg', 'cat': 'Chronic', 'crit': 'Medium'},
    {'name': 'Ringer Lactate', 'cat': 'Fluids', 'crit': 'High'},
    {'name': 'Normal Saline 0.9%', 'cat': 'Fluids', 'crit': 'Medium'},
    {'name': 'Epinephrine', 'cat': 'Emergency', 'crit': 'High'},
    {'name': 'BCG Vaccine', 'cat': 'Vaccine', 'crit': 'High'},
    {'name': 'Polio Vaccine', 'cat': 'Vaccine', 'crit': 'High'},
    {'name': 'Surgical Masks', 'cat': 'Consumable', 'crit': 'Low'},
    {'name': 'Sterile Gloves', 'cat': 'Consumable', 'crit': 'Low'},
    {'name': 'Disposable Syringes 3ml', 'cat': 'Consumable', 'crit': 'Low'},
    {'name': 'IV Cannula', 'cat': 'Consumable', 'crit': 'Medium'},
    {'name': 'HIV Rapid Test Kit', 'cat': 'Diagnostic', 'crit': 'High'},
    {'name': 'Malaria RDT', 'cat': 'Diagnostic', 'crit': 'High'},
    {'name': 'Blood Glucose Strips', 'cat': 'Diagnostic', 'crit': 'Medium'},
    {'name': 'Paracetamol 500mg', 'cat': 'General', 'crit': 'Low'},
    {'name': 'Ibuprofen 400mg', 'cat': 'General', 'crit': 'Low'},
    {'name': 'Oral Rehydration Salts', 'cat': 'General', 'crit': 'Medium'},
    {'name': 'TB-Kit Adult', 'cat': 'Infectious', 'crit': 'High'},
    {'name': 'Artemether (Malaria)', 'cat': 'Infectious', 'crit': 'High'},
    {'name': 'Folic Acid', 'cat': 'Maternal', 'crit': 'Low'}
]

def generate_inventory():
    try:
        df_facilities = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found.")
        return

    inventory_rows = []
    
    # Identify Victim/Savior IDs for Oxytocin (same logic as before)
    victim_ids = df_facilities['FACILITY_ID'].head(3).tolist()
    savior_ids = []
    for vid in victim_ids:
        victim_row = df_facilities[df_facilities['FACILITY_ID'] == vid].iloc[0]
        v_loc = (victim_row['LATITUDE'], victim_row['LONGITUDE'])
        min_dist, nearest = 9999, None
        for _, row in df_facilities.iterrows():
            if row['FACILITY_ID'] == vid: continue
            dist = geodesic(v_loc, (row['LATITUDE'], row['LONGITUDE'])).km
            if dist < min_dist:
                min_dist, nearest = dist, row['FACILITY_ID']
        if nearest: savior_ids.append(nearest)

    current_date = datetime.now().strftime('%Y-%m-%d')
    
    for _, fac in df_facilities.iterrows():
        fid = fac['FACILITY_ID']
        
        # Select at least 20 random items for this hospital
        num_items = random.randint(20, 25)
        selected_items = random.sample(MASTER_ITEMS, num_items)
        
        # Ensure 'Oxytocin' is always included if the hospital is a victim or savior
        selected_names = [i['name'] for i in selected_items]
        if (fid in victim_ids or fid in savior_ids) and 'Oxytocin Injection' not in selected_names:
            selected_items.append(MASTER_ITEMS[0]) # Add Oxytocin manually

        for item in selected_items:
            # Default healthy stock
            opening = random.randint(100, 400)
            received = random.randint(0, 30)
            issued = random.randint(5, 25)
            lead_time = random.randint(2, 6)
            
            # Crisis Scenario Logic
            if item['name'] == 'Oxytocin Injection':
                if fid in victim_ids:
                    opening, received, issued = 10, 0, 10 # Crisis
                elif fid in savior_ids:
                    opening, received, issued = 600, 0, 5 # Surplus
            
            closing = max(0, opening + received - issued)

            inventory_rows.append({
                'RECORD_ID': str(uuid.uuid4()),
                'DATE': current_date,
                'FACILITY_ID': fid,
                'ITEM_NAME': item['name'],
                'CATEGORY': item['cat'],
                'OPENING_STOCK': opening,
                'RECEIVED_QTY': received,
                'ISSUED_QTY': issued,
                'CLOSING_STOCK': closing,
                'LEAD_TIME_DAYS': lead_time,
                'CRITICALITY_LEVEL': item['crit']
            })

    pd.DataFrame(inventory_rows).to_csv(OUTPUT_FILE, index=False)
    print(f"Generated {len(inventory_rows)} records for {len(df_facilities)} hospitals.")

if __name__ == "__main__":
    generate_inventory()