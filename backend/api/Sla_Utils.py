import pandas as pd
import numpy as np
from datetime import timedelta
import warnings

warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)

HOLIDAYS = set()
MIN_SLA_MINUTES = 5

COLUMNS = [
    "unique_id", "ESS Client", "Date of go ahead", "Date of completion at cmpak end work",
    "Service", "Bandwidth(Mbps)", "Location", "Time of go ahead", "Deployment Time",
    "Category", "Wired/Wireless", "Last Mile vendor ",  # ✅ ADD THIS
    "work_order", "WORK ORDER APPROVAL",
    "Date/Time of go ahead", "Date/Time of Deployment", "Working Hours", "Remarks"
]


WORK_START = 9
WORK_END_MON_THU = 18
WORK_END_FRI = 18


# ----------------------------
# HELPERS (unchanged)
# ----------------------------
def safe_lower(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().replace("nan", "")

def to_timedelta_safe(series: pd.Series) -> pd.Series:
    return pd.to_timedelta(series.astype(str), errors="coerce")

def to_datetime_safe(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")

def clamp_negative_to_5_minutes(value: float, unit: str) -> float:
    if pd.isna(value):
        return value
    if value < 0:
        if unit == "hours":
            return round(MIN_SLA_MINUTES / 60, 4)
        if unit == "days":
            return round(MIN_SLA_MINUTES / (60 * 24), 6)
    return value

def adjust_working_hours(dt):
    if pd.isna(dt):
        return dt

    while True:
        while dt.weekday() >= 5:
            dt += timedelta(days=1)
            dt = dt.replace(hour=WORK_START, minute=0, second=0, microsecond=0)

        while dt.date() in HOLIDAYS:
            dt += timedelta(days=1)
            dt = dt.replace(hour=WORK_START, minute=0, second=0, microsecond=0)

        end_hour = WORK_END_FRI if dt.weekday() == 4 else WORK_END_MON_THU

        if dt.hour + dt.minute / 60 >= end_hour:
            dt += timedelta(days=1)
            dt = dt.replace(hour=WORK_START, minute=0, second=0, microsecond=0)
            continue

        if dt.hour + dt.minute / 60 < WORK_START:
            dt = dt.replace(hour=WORK_START, minute=0, second=0, microsecond=0)

        break

    return dt

def business_hours_between(start_dt, end_dt) -> float:
    start_dt = adjust_working_hours(start_dt)
    end_dt = adjust_working_hours(end_dt)

    if pd.isna(start_dt) or pd.isna(end_dt):
        return np.nan

    if start_dt > end_dt:
        return 0.0

    if start_dt.date() == end_dt.date():
        return max((end_dt - start_dt).total_seconds() / 3600, 0)

    start_end_hour = WORK_END_FRI if start_dt.weekday() == 4 else WORK_END_MON_THU
    start_day_end = start_dt.replace(hour=start_end_hour, minute=0, second=0, microsecond=0)
    start_day_hours = max((start_day_end - start_dt).total_seconds() / 3600, 0)

    end_day_start = end_dt.replace(hour=WORK_START, minute=0, second=0, microsecond=0)
    end_day_hours = max((end_dt - end_day_start).total_seconds() / 3600, 0)

    max_end_day = (WORK_END_FRI - WORK_START) if end_dt.weekday() == 4 else (WORK_END_MON_THU - WORK_START)
    end_day_hours = min(end_day_hours, max_end_day)

    total_intermediate = 0.0
    cur = start_dt.date() + timedelta(days=1)
    while cur < end_dt.date():
        if cur.weekday() < 5 and cur not in HOLIDAYS:
            total_intermediate += (WORK_END_FRI - WORK_START) if cur.weekday() == 4 else (WORK_END_MON_THU - WORK_START)
        cur += timedelta(days=1)

    return max(start_day_hours + total_intermediate + end_day_hours, 0)

def build_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["Date of go ahead"] = to_datetime_safe(df["Date of go ahead"])
    df["Date of completion at cmpak end work"] = to_datetime_safe(df["Date of completion at cmpak end work"])

    df["Time of go ahead"] = to_timedelta_safe(df["Time of go ahead"])
    df["Deployment Time"] = to_timedelta_safe(df["Deployment Time"])

    df["WORK ORDER APPROVAL"] = to_datetime_safe(df.get("WORK ORDER APPROVAL"))
    df["E-Bidding Go ahead Date"] = to_datetime_safe(df.get("E-Bidding Go ahead Date"))

    df["Date/Time of go ahead"] = df["Date of go ahead"] + df["Time of go ahead"]
    df["Date/Time of Deployment"] = df["Date of completion at cmpak end work"] + df["Deployment Time"]

    return df


# ----------------------------
# SLA CALCULATIONS (unchanged)
# ----------------------------
def calculate_sla(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    df = df.copy()

    if mode == "vendor_new_link_days":
        delta = df["Date/Time of Deployment"] - df["Date/Time of go ahead"]
        df["Working Hours"] = (delta.dt.total_seconds() / 86400).round(4)
        df["Working Hours"] = df["Working Hours"].apply(lambda x: clamp_negative_to_5_minutes(x, "days"))

    elif mode == "existing_hours":
        delta = df["Date/Time of Deployment"] - df["WORK ORDER APPROVAL"]
        df["Working Hours"] = (delta.dt.total_seconds() / 3600).round(2)
        df["Working Hours"] = df["Working Hours"].apply(lambda x: clamp_negative_to_5_minutes(x, "hours"))

    elif mode == "ftth_days":
        delta = df["Date/Time of Deployment"] - df["E-Bidding Go ahead Date"]
        df["Working Hours"] = (delta.dt.total_seconds() / 86400).round(4)
        df["Working Hours"] = df["Working Hours"].apply(lambda x: clamp_negative_to_5_minutes(x, "days"))

    else:
        raise ValueError("Invalid SLA mode")

    return df

def calculate_customer_own_business_hours(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # IMPORTANT: keep the SAME string-concat parsing as your working script
    df["go_ahead"] = pd.to_datetime(
        df["Date of go ahead"].astype(str) + " " + df["Time of go ahead"].astype(str),
        errors="coerce"
    )
    df["deploy"] = pd.to_datetime(
        df["Date of completion at cmpak end work"].astype(str) + " " + df["Deployment Time"].astype(str),
        errors="coerce"
    )

    df["Working Hours"] = df.apply(lambda r: business_hours_between(r["go_ahead"], r["deploy"]), axis=1)
    df["Working Hours"] = df["Working Hours"].round(2)
    df["Working Hours"] = df["Working Hours"].apply(lambda x: clamp_negative_to_5_minutes(x, "hours"))
    return df


# ----------------------------
# PUBLIC: BACKEND ENTRYPOINT (IN-MEMORY)
# ----------------------------
def compute_sla_working_hours(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    IDENTICAL behavior to your working script, but:
    - No file reading
    - No file writing
    - Takes df_input already in memory
    - Returns df_final (same schema)
    """

    df = df_input.copy()

    # same normalization
    df["Category"] = safe_lower(df["Category"])
    df["Last Mile vendor "] = safe_lower(df["Last Mile vendor "])
    df["Service"] = df["Service"].astype(str).str.strip()

    # Split (same as script)
    df_existing_links = df[df["Category"].isin([
        "termination", "upgradation simple", "upgradation with changes", "downgradation"
    ])].copy()

    df_new_links_customer_own = df[
        (df["Category"] == "new link provisioning") &
        (df["Last Mile vendor "] == "customer's own fiber link")
    ].copy()

    df_new_links_vendor = df[
        (df["Category"] == "new link provisioning") &
        (df["Last Mile vendor "] != "customer's own fiber link") &
        (df["Service"] != "FTTH")          # SAME as your script (case-sensitive)
    ].copy()

    df_ftth_new_links = df[
        (df["Category"] == "new link provisioning") &
        (df["Service"] == "FTTH")          # SAME as your script (case-sensitive)
    ].copy()

    # Build datetime columns (same)
    df_existing_links = build_datetime_columns(df_existing_links)
    df_new_links_vendor = build_datetime_columns(df_new_links_vendor)
    df_ftth_new_links = build_datetime_columns(df_ftth_new_links)

    # SLA calculations (same)
    df_existing_links = calculate_sla(df_existing_links, "existing_hours")
    df_new_links_vendor = calculate_sla(df_new_links_vendor, "vendor_new_link_days")
    df_ftth_new_links = calculate_sla(df_ftth_new_links, "ftth_days")

    # Customer own (same)
    df_new_links_customer_own = calculate_customer_own_business_hours(df_new_links_customer_own)

    # Ensure required columns exist (same)
    for d in [df_new_links_customer_own, df_new_links_vendor, df_existing_links, df_ftth_new_links]:
        for c in COLUMNS:
            if c not in d.columns:
                d[c] = np.nan

    # Order columns (same)
    df_new_links_customer_own = df_new_links_customer_own[COLUMNS]
    df_new_links_vendor = df_new_links_vendor[COLUMNS]
    df_existing_links = df_existing_links[COLUMNS]
    df_ftth_new_links = df_ftth_new_links[COLUMNS]

    parts = [df_new_links_customer_own, df_new_links_vendor, df_existing_links, df_ftth_new_links]
    parts = [p for p in parts if not p.empty]

    df_final = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLUMNS)

    df_final.to_excel('Now checkthis.xlsx')

    return df_final
