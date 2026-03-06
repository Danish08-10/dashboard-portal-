# api/breach_utils.py
import pandas as pd
import numpy as np

from .Sla_targets import (
    EXISTING_TARGET_HOURS,
    CUSTOMER_OWN_TARGET_HOURS,
    FTTH_TARGET_DAYS,
    VENDOR_NEWLINK_TARGET_DAYS,
)

def _norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().replace("nan", "")

def apply_targets_and_breaches(df_final: pd.DataFrame) -> pd.DataFrame:
    df = df_final.copy()

    cat = _norm(df["Category"])
    service = df["Service"].astype(str).str.strip()  # keep original case for FTTH
    vendor = _norm(df.get("Last Mile vendor ", pd.Series([""] * len(df))))
    wired_wireless = _norm(df.get("Wired/Wireless", pd.Series([""] * len(df))))

    df["SLA_Target"] = np.nan
    df["SLA_Unit"] = ""
    df["SLA_Breached"] = False

    # 1) Existing links (HOURS)
    for k, target in EXISTING_TARGET_HOURS.items():
        m = (cat == k)
        df.loc[m, "SLA_Target"] = float(target)
        df.loc[m, "SLA_Unit"] = "hours"

    # 2) New link provisioning
    m_new = (cat == "new link provisioning")

    # 2a) Customer own (HOURS)
    m_customer_own = m_new & (vendor == "customer's own fiber link")
    df.loc[m_customer_own, "SLA_Target"] = float(CUSTOMER_OWN_TARGET_HOURS)
    df.loc[m_customer_own, "SLA_Unit"] = "hours"

    # 2b) FTTH (DAYS)
    m_ftth = m_new & (service == "FTTH")
    df.loc[m_ftth, "SLA_Target"] = float(FTTH_TARGET_DAYS)
    df.loc[m_ftth, "SLA_Unit"] = "days"

    # 2c) Vendor new links (DAYS) based on Wired/Wireless
    m_vendor_new = m_new & (~m_customer_own) & (~m_ftth)
    for ww, target_days in VENDOR_NEWLINK_TARGET_DAYS.items():
        m = m_vendor_new & (wired_wireless == ww)
        df.loc[m, "SLA_Target"] = float(target_days)
        df.loc[m, "SLA_Unit"] = "days"

    # Breach
    wh = pd.to_numeric(df["Working Hours"], errors="coerce")
    tgt = pd.to_numeric(df["SLA_Target"], errors="coerce")

    ok = wh.notna() & tgt.notna()
    df.loc[ok, "SLA_Breached"] = wh[ok] > tgt[ok]

    return df

def sla_kpis_and_breached_table(df_with_breaches: pd.DataFrame):
    df = df_with_breaches.copy()

    wh = pd.to_numeric(df["Working Hours"], errors="coerce")
    tgt = pd.to_numeric(df["SLA_Target"], errors="coerce")

    total_mask = wh.notna() & tgt.notna()
    total_sla_links = int(total_mask.sum())

    breached_mask = total_mask & (df["SLA_Breached"] == True)
    achieved_mask = total_mask & (df["SLA_Breached"] == False)

    breached_count = int(breached_mask.sum())
    achieved_count = int(achieved_mask.sum())

    achieved_pct = round((achieved_count / total_sla_links) * 100, 2) if total_sla_links else 0.0
    breached_pct = round((breached_count / total_sla_links) * 100, 2) if total_sla_links else 0.0

    breached_df = df[breached_mask].copy()
    breached_df['Target SLA'] = (
    breached_df['SLA_Target'].astype(str) + " " +
    breached_df['SLA_Unit'].astype(str)
)


    cols = [
        "unique_id",
        "ESS Client",
        "Category",
        "Service",
        "Last Mile vendor ",
        "Working Hours",
        "Target SLA",
        "Remarks",
        "Location",
        "Date/Time of go ahead",       # ✅
        "Date/Time of Deployment",     # ✅
    ]
    cols = [c for c in cols if c in breached_df.columns]

    # ✅ Format datetime nicely (no T00:00:00)
    for c in ["Date/Time of go ahead", "Date/Time of Deployment"]:
        if c in breached_df.columns:
            breached_df[c] = pd.to_datetime(breached_df[c], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    breached_table = breached_df[cols].replace({np.nan: ""}).values.tolist()

    return {
        "total_sla_links": total_sla_links,
        "sla_achieved_count": achieved_count,
        "sla_achieved_pct": achieved_pct,
        "sla_breached_count": breached_count,
        "sla_breached_pct": breached_pct,
        "breached_columns": cols,
        "breached_table": breached_table,
    }
