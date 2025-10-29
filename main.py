import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="Car Condition Entry",
    page_icon="🚗",
    layout="centered"
)

# Set up BigQuery credentials
@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(
        'service_account.json'
    )
    return bigquery.Client(credentials=credentials, project='pricing-338819')

client = get_bigquery_client()
table_id = 'pricing-338819.wholesale_test.retail_car_growth_comments'

# Title
st.title("🚗 Car Condition Entry Form")
st.markdown("---")

# Create the form
with st.form("car_condition_form", clear_on_submit=True):
    st.subheader("Enter Car Details")
    
    # Input fields
    car_name = st.text_input(
        "Car Name",
        placeholder="e.g., C-13383",
        help="Enter the car identifier"
    )
    
    condition = st.text_area(
        "Condition",
        placeholder="Enter the car condition details...",
        help="Describe the condition of the car",
        height=150
    )
    
    # Submit button
    submitted = st.form_submit_button("Submit", use_container_width=True, type="primary")
    
    if submitted:
        # Validation
        if not car_name.strip():
            st.error("❌ Please enter a car name")
        elif not condition.strip():
            st.error("❌ Please enter the condition")
        else:
            try:
                # Prepare data
                today = date.today()
                data = {
                    'date': [today],
                    'car_name': [car_name.strip()],
                    'condition': [condition.strip()]
                }
                df = pd.DataFrame(data)
                
                # Upload to BigQuery
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema=[
                        bigquery.SchemaField("date", "DATE"),
                        bigquery.SchemaField("car_name", "STRING"),
                        bigquery.SchemaField("condition", "STRING"),
                    ]
                )
                
                with st.spinner("Submitting..."):
                    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                    job.result()  # Wait for completion
                
                st.success(f"✅ Successfully submitted!\n\n**Car:** {car_name}\n\n**Date:** {today}")
                st.balloons()
                
            except Exception as e:
                st.error(f"❌ Error submitting data: {str(e)}")

# Display recent entries
st.markdown("---")
st.subheader("📋 Recent Entries (Last 10)")

try:
    query = f"""
    SELECT date, car_name, condition 
    FROM `{table_id}` 
    ORDER BY date DESC 
    LIMIT 10
    """
    df_recent = client.query(query).to_dataframe()
    
    if not df_recent.empty:
        # Format the dataframe for display
        df_recent['date'] = pd.to_datetime(df_recent['date']).dt.strftime('%Y-%m-%d')
        st.dataframe(
            df_recent,
            use_container_width=True,
            hide_index=True,
            column_config={
                "date": st.column_config.TextColumn("Date", width="small"),
                "car_name": st.column_config.TextColumn("Car Name", width="small"),
                "condition": st.column_config.TextColumn("Condition", width="large"),
            }
        )
    else:
        st.info("No entries yet")
        
except Exception as e:
    st.warning(f"Could not load recent entries: {str(e)}")

# Footer
st.markdown("---")
st.caption("🔒 Data is securely stored in BigQuery")

