import pandas as pd
import streamlit as st
import boto3
import io
import matplotlib.pyplot as plt
import os

# AWS Credentials & S3 Config
aws_access_key_id = st.secrets["aws_access_key_id"]
aws_secret_access_key = st.secrets["aws_secret_access_key"]
aws_region = st.secrets["aws_region"]
s3_bucket = st.secrets["s3_bucket"]

# Create S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# --------- Step 1: Load Data from S3 ---------
@st.cache_data
def load_all_data():
    data = {}

    def load_from_prefix(prefix):
        dfs = []
        res = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        for obj in res.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                file = s3.get_object(Bucket=s3_bucket, Key=key)
                df = pd.read_csv(io.BytesIO(file['Body'].read()))
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    data['meetings'] = load_from_prefix("transformed_data/meetings_transformed/")
    data['sessions'] = load_from_prefix("transformed_data/sessions_transformed/")
    data['drivers'] = load_from_prefix("transformed_data/drivers_transformed/")
    data['laps'] = load_from_prefix("transformed_data/laps_transformed/")

    return data

# --------- Step 2: Load Data ---------
with st.spinner("Loading F1 data..."):
    data = load_all_data()
    meetings_df = data['meetings']
    sessions_df = data['sessions']
    drivers_df = data['drivers']
    laps_df = data['laps']

st.success("Data loaded successfully!")

# --------- Step 3: Interactive Dashboard ---------
st.header("Sector-wise Driver Performance (RACE only)")

# --- Select Race ---
selected_race = st.selectbox("Select Race", meetings_df["meeting_name"].dropna().unique())

# Get meeting_key for selected race
selected_meeting = meetings_df[meetings_df["meeting_name"] == selected_race]
if selected_meeting.empty:
    st.warning("No data found for this race.")
    st.stop()

meeting_key = selected_meeting["meeting_key"].values[0]

# --- Filter sessions to only Race session ---
race_sessions = sessions_df[
    (sessions_df["meeting_key"] == meeting_key) &
    (sessions_df["session_type"].str.lower() == "race")
]["session_key"].unique()

if len(race_sessions) == 0:
    st.warning("No Race session found for this meeting.")
    st.stop()

# Filter laps only for Race session
filtered_laps_df = laps_df[laps_df["session_key"].isin(race_sessions)]

# Join with drivers
filtered_laps_df = filtered_laps_df.merge(drivers_df, on="driver_number", how="left")

# --- Select Driver ---
available_drivers = filtered_laps_df["full_name"].dropna().unique()
selected_driver = st.selectbox("Select Driver", available_drivers)

# --- Select Sector ---
sector_options = ["duration_sector_1", "duration_sector_2", "duration_sector_3"]
sector_labels = {"duration_sector_1": "Sector 1", "duration_sector_2": "Sector 2", "duration_sector_3": "Sector 3"}
selected_sector = st.selectbox("Select Sector", sector_options, format_func=lambda x: sector_labels[x])

# --- Filter by Driver ---
driver_laps_df = filtered_laps_df[filtered_laps_df["full_name"] == selected_driver].copy()

# Ensure numeric lap numbers and drop any rows missing needed data
driver_laps_df['lap_number'] = pd.to_numeric(driver_laps_df['lap_number'], errors='coerce')
driver_laps_df = driver_laps_df.dropna(subset=['lap_number', selected_sector])

# Sort by lap number
driver_laps_df = driver_laps_df.sort_values("lap_number")

# Deduplicate to ensure one row per lap and sector to avoid duplicate slow lap entries
driver_laps_df = driver_laps_df.drop_duplicates(subset=["lap_number", selected_sector])

# Calculate 85th percentile & delta
p85_value = driver_laps_df[selected_sector].quantile(0.85)
driver_laps_df["delta_from_p85"] = driver_laps_df[selected_sector] - p85_value
driver_laps_df["is_slow"] = driver_laps_df["delta_from_p85"] > 0

# Plot with slow laps highlighted
fig, ax = plt.subplots(figsize=(10, 5))

# Plot all laps
ax.plot(driver_laps_df["lap_number"], driver_laps_df[selected_sector], label="Sector Time", color="blue", marker="o")

# Highlight slow laps
slow_laps_df = driver_laps_df[driver_laps_df["is_slow"]]
ax.scatter(slow_laps_df["lap_number"], slow_laps_df[selected_sector], color="red", label="Slow Laps")

# Add 85th percentile line
ax.axhline(y=p85_value, color="orange", linestyle="--", label=f"85th Percentile ({p85_value:.2f}s)")

# Format
ax.set_title(f"{selected_driver} - {sector_labels[selected_sector]}")
ax.set_xlabel("Lap Number")
ax.set_ylabel("Sector Duration (s)")
ax.set_xticks(range(0, int(driver_laps_df["lap_number"].max()) + 1, 10))
ax.set_yticks(range(0, int(driver_laps_df[selected_sector].max()) + 5, 5))
ax.legend()
ax.grid(True)

st.pyplot(fig)

# --- Display Slow Laps Table ---
st.subheader("Laps Slower than 85th Percentile")

if slow_laps_df.empty:
    st.info("No slow laps detected (above 85th percentile).")
else:
    display_cols = ["lap_number", selected_sector, "delta_from_p85"]
    slow_laps_display = slow_laps_df[display_cols].rename(columns={
        "lap_number": "Lap Number",
        selected_sector: "Sector Time (s)",
        "delta_from_p85": "Delta from P85 (s)"
    })
    slow_laps_display["Sector Time (s)"] = slow_laps_display["Sector Time (s)"].round(3)
    slow_laps_display["Delta from P85 (s)"] = slow_laps_display["Delta from P85 (s)"].round(3)

    st.dataframe(slow_laps_display.reset_index(drop=True), use_container_width=True)

