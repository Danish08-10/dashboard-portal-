import pandas as pd
import datetime
import random
from sqlalchemy import create_engine

user = "postgres.kimsluaqkkipyzpnfpxl"
password = "CxzxdPqFKciVppqT"
host = "aws-1-ap-southeast-2.pooler.supabase.com"
port = "5432"
database = "CBS Central"
table_name = "insale"

try:
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/postgres",
        pool_pre_ping=True
    )

    with engine.connect() as conn:
        print("Supabase Engine Connected Successfully")

except Exception as e:
    print(f"Error is this: {e}")




df_deployment = pd.read_sql("SELECT * FROM insale", engine)
df_invoices_mrc = pd.read_sql('SELECT * FROM "MRC"', engine)
df_invoice = pd.read_sql('SELECT * FROM "Vendor_Status"', engine)


# df_deployment=pd.read_excel(r"C:\Users\danish.azhar\Desktop\Updated Deployment-Tracker.xlsx",sheet_name='Deployment-Tracker-20251121')
# df_invoices_mrc=pd.read_excel(r"C:\Users\danish.azhar\Desktop\Invoices\MRC & OTC of all Links.xlsx",sheet_name='Sheet1')
# df_invoice=pd.read_excel(r"C:\Users\danish.azhar\Desktop\Vendor Links Working\Invoices status.xlsx",sheet_name='Sheet1')





df_deployment.drop('Sr.No',inplace=True,axis=1)
df_deployment['Date of go ahead'] = pd.to_datetime(df_deployment['Date of go ahead'], errors='coerce')
df_deployment['Date of completion at cmpak end work'] = pd.to_datetime(df_deployment['Date of completion at cmpak end work'], errors='coerce')

# Change Report Date From Here
start_date = "1-feb-2026"
end_date='28-Feb-2026 23:59:59'
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)


df_sect1 = df_deployment[(df_deployment['Date of go ahead'] >= start_date) & (df_deployment['Date of go ahead'] <= end_date)]
df_sect1=df_sect1[df_sect1['Remarks']!='this is done intentionally just to sync the sd']

df_sect2=df_deployment[(df_deployment['Date of completion at cmpak end work'] >= start_date) & (df_deployment['Date of completion at cmpak end work'] <= end_date)]
df_sect2=df_sect2[df_sect2['Remarks']!='this is done intentionally just to sync the sd']

df_sect8 = df_deployment[
    (df_deployment['Date of completion at cmpak end work'] <= end_date) &
    (df_deployment['Remarks'] != 'this is done intentionally just to sync the sd')
].copy()

def section1(df_deployment):
    df_deployment=df_deployment[df_deployment['Remarks']!='this is done intentionally just to sync the sd']
    df_deployment['Case Closed on GEC Group/Fixed Telecom Group'] = df_deployment['Case Closed on GEC Group/Fixed Telecom Group'].str.lower()

    pending_customer_end = df_deployment[
        (df_deployment['Date of Deployment completion'] == 'Pending at Customer end') &
        ((df_deployment['Case Closed on GEC Group/Fixed Telecom Group'].isna()) |
         ~(df_deployment['Case Closed on GEC Group/Fixed Telecom Group'] == 'yes'))
    ]
    pending_customer_end_count = pending_customer_end['Date of Deployment completion'].count()
    status_counts = df_deployment['Status'].value_counts()
    total_projects = status_counts.sum()
    completed_projects = status_counts.get('Completed', 0)
    inprogress_projects = status_counts.get('Inprogress', 0)
    total_pending_projects = inprogress_projects
    not_feasible=status_counts.get('Not Feasible',0)
    rejected=status_counts.get('Rejected',0)
    return total_projects, completed_projects, total_pending_projects, inprogress_projects, pending_customer_end_count,not_feasible,rejected

def section2(df_deployment):
    df_inprogress = df_deployment[df_deployment['Status'] == 'Inprogress'].copy()
    if df_inprogress.empty:
        return []

    try:
        required_cols = ['Date of go ahead', 'E-Bidding Go ahead Date']
        for col in required_cols:
            if col not in df_inprogress.columns:
                raise KeyError(f"Missing required column: {col}")

        # Convert to datetime
        df_inprogress['Date of go ahead'] = pd.to_datetime(df_inprogress['Date of go ahead'], errors='coerce')
        df_inprogress['E-Bidding Go ahead Date'] = pd.to_datetime(df_inprogress['E-Bidding Go ahead Date'], errors='coerce')

        # Calculate Aging Time
        termination_mask = df_inprogress['Category'] == 'Termination'
        df_inprogress['Aging Time'] = pd.NA  # keep numeric type
        now = pd.Timestamp.now()

        df_inprogress.loc[termination_mask & df_inprogress['E-Bidding Go ahead Date'].notna(), 'Aging Time'] = (
            (now - df_inprogress.loc[termination_mask & df_inprogress['E-Bidding Go ahead Date'].notna(), 'E-Bidding Go ahead Date']).dt.days
        )

        df_inprogress.loc[~termination_mask & df_inprogress['Date of go ahead'].notna(), 'Aging Time'] = (
            (now - df_inprogress.loc[~termination_mask & df_inprogress['Date of go ahead'].notna(), 'Date of go ahead']).dt.days
        )

        # Keep only necessary columns
        columns = ['ESS Client', 'Status', 'Aging Time', 'Service', 'Bandwidth(Mbps)', 'Category', 'Remarks', 'Location']
        missing_columns = [col for col in columns if col not in df_inprogress.columns]
        if missing_columns:
            raise KeyError(f"Missing columns in dataframe: {missing_columns}")

        df_inprogress = df_inprogress[columns].fillna('')
        return df_inprogress.values.tolist()

    except Exception as e:
        return {"error": str(e)}


def section4(df_deployment):
    df_deployment = df_deployment.copy()

    # only completed
    df_deployment = df_deployment[df_deployment['Status'] == 'Completed']
    df_deployment.to_excel("Check_this2.xlsx")

    # normalize
    caf = df_deployment['CAF Signed'].astype(str).str.strip().str.lower()

    # counts
    no_of_awaited = caf.eq('caf awaited').sum()

    no_of_signed = caf.isin([
        'caf signed',
        'caf signed by rcbs',
        'not required'
    ]).sum()

    return no_of_awaited, no_of_signed





def section5(df_deployment):
    from pandas import isna

    # Filter rows where completion date exists
    df_filtered = df_deployment[df_deployment['Date of completion at cmpak end work'].notna()].copy()

    # Ensure both identifier columns exist
    if 'Unique Identifier' not in df_deployment.columns or 'unique_id' not in df_deployment.columns:
        raise KeyError("Required columns ('Unique Identifier', 'unique_id') are missing.")

    # Combine unique identifiers
    df_filtered['combined'] = df_filtered['Unique Identifier'].fillna(df_filtered['unique_id'])

    # Count missing and filled
    missing = df_filtered['combined'].isna().sum()
    filled = df_filtered['combined'].notna().sum()

    return missing, filled

def section6(df_deployment):
    df_deployment['Case Closed on GEC Group/Fixed Telecom Group'] = df_deployment['Case Closed on GEC Group/Fixed Telecom Group'].str.strip().str.lower()

    if 'Status' in df_deployment.columns:
        in_progress_count = (df_deployment['Status'] == 'Inprogress').sum()
    else:
        in_progress_count = 0  
    
    not_handed_over = (
        df_deployment['Case Closed on GEC Group/Fixed Telecom Group'].isna().sum()
        + (df_deployment['Case Closed on GEC Group/Fixed Telecom Group'] == 'no').sum()
        - in_progress_count
    )
    df_not_handed=df_deployment[
    (df_deployment['Case Closed on GEC Group/Fixed Telecom Group'].isna() | 
     (df_deployment['Case Closed on GEC Group/Fixed Telecom Group'] == 'no')) & 
    (df_deployment['Status'] != 'Inprogress')
]
    handed_over = ((df_deployment['Case Closed on GEC Group/Fixed Telecom Group'] == 'yes').sum()+ (df_deployment['Case Closed on GEC Group/Fixed Telecom Group'] == 'not required').sum())
    columns = ['ESS Client', 'Status', 'Date of go ahead', 'Remarks','Category']
    df_not_handed=df_not_handed[columns]
    not_handed_over_table=df_not_handed.to_html(index=False, border=1)
    not_handed_over_table=df_not_handed.fillna('')
    not_handed_over_table=not_handed_over_table.values.tolist()
    
    return not_handed_over, handed_over,not_handed_over_table

def section7(df_deployment):
    df_deployment = df_deployment.copy()
    df_deployment = df_deployment[df_deployment['Status'].astype(str).str.strip().str.lower() == 'completed']
    df_deployment['Case closed on CSP'] = df_deployment['Case closed on CSP'].astype(str).str.strip().str.lower()

    status_counts = df_deployment['Case closed on CSP'].value_counts()
    not_closed = status_counts.get('opened', 0) + df_deployment['Case closed on CSP'].isna().sum()
    closed = status_counts.get('closed', 0)
    total = df_deployment['Case closed on CSP'].ne('not required').sum()

    if 'Date of go ahead' in df_deployment.columns:
        df_deployment['Date of go ahead'] = pd.to_datetime(df_deployment['Date of go ahead'], errors='coerce')
        df_deployment['Aging'] = (datetime.datetime.now() - df_deployment['Date of go ahead']).dt.days

    not_closed_df = df_deployment[df_deployment['Case closed on CSP'].isna() | (df_deployment['Case closed on CSP'] == 'opened')]

    columns = ['ESS Client', 'Status', 'Date of go ahead', 'Aging', 'Remarks']
    existing_columns = [col for col in columns if col in not_closed_df.columns]

    not_closed_df = not_closed_df[existing_columns]
    not_closed_table = not_closed_df.to_html(index=False, border=1)
    not_closed_table=not_closed_df.fillna('')
    not_closed_table=not_closed_table.values.tolist()

    return total, closed, not_closed, not_closed_table



def section8(df_deployment, end_date=None):
    import pandas as pd

    df = df_deployment.copy()

    # ----------------------------
    # Dates
    # ----------------------------
    df['Date of completion at cmpak end work'] = pd.to_datetime(
        df['Date of completion at cmpak end work'], errors='coerce'
    )

    today = pd.Timestamp.today().normalize()
    if end_date is None:
        end_date = today
    else:
        end_date = pd.to_datetime(end_date).normalize()

    start_window = end_date - pd.DateOffset(months=6)

    # Drop invalid required fields
    df = df.dropna(subset=['Date of completion at cmpak end work', 'Category'])

    # Filter date window (last 6 months up to end_date)
    df = df[
        (df['Date of completion at cmpak end work'] >= start_window) &
        (df['Date of completion at cmpak end work'] <= end_date)
    ]

    # ----------------------------
    # Category normalization (FIX)
    # ----------------------------
    df['Category'] = df['Category'].astype(str).str.strip().str.lower()

    # Map variants to unified buckets
    df['Category'] = df['Category'].replace({
        'upgradation simple': 'upgradation',
        'upgradation with changes': 'upgradation',
        'new link provisioning': 'new link provisioning',
        'demo new link provisioning': 'new link provisioning',
    })
    # Keep only relevant buckets
    df = df[df['Category'].isin(['new link provisioning', 'upgradation'])]

    # ----------------------------
    # Month label + sort
    # ----------------------------
    df['Month_Sort'] = df['Date of completion at cmpak end work'].dt.to_period('M')
    df['name'] = df['Month_Sort'].dt.strftime('%b-%Y')

    # Pivot
    result = df.groupby(['Month_Sort', 'name', 'Category']).size().unstack(fill_value=0)

    # Ensure required columns always exist
    for col in ['new link provisioning', 'upgradation']:
        if col not in result.columns:
            result[col] = 0

    # Sort by month
    result = result.sort_index()

    # Final records
    out = (
        result.reset_index()
        .rename(columns={
            'new link provisioning': 'New_Links',
            'upgradation': 'Upgraded',
        })
    )

    return out[['name', 'New_Links', 'Upgraded']].to_dict(orient='records')

def random_color():
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))
def section9(df_deployment):
    allowed_categories = [
        "New Link Provisioning",
        "Upgradation Simple",
        "Upgradation with changes"
    ]
    filtered_df = df_deployment[df_deployment['Category'].isin(allowed_categories)]

    service_counts = filtered_df['Service'].value_counts()

    chart_data = [
        {
            "name": service,
            "value": count,
            "color": random_color()
        }
        for service, count in service_counts.items()
    ]

    return chart_data


def section10(df_deployment):
    total_vendors = df_deployment['Last Mile vendor'].nunique()
    total_links = df_deployment['ESS Client'].count()

    wireless_links = df_deployment[df_deployment['Wired/Wireless'] == 'Wireless']['ESS Client'].count()
    wired_links = df_deployment[df_deployment['Wired/Wireless'] == 'Wired']['ESS Client'].count()

    return total_vendors,total_links,wireless_links,wired_links
    

def section11(df_deployment):
    columns = ['Last Mile vendor', 'Last Invoice (WCC Issued)', 'Status 3']
    df_deployment = df_deployment[columns].copy()

    def to_mon_yy(x):
        # handle NaN/None/empty
        if pd.isna(x) or str(x).strip() == "" or str(x).strip() == "-":
            return "-"

        s = str(x).strip()

        # if already like "Oct-25" or "Sept-25" keep as-is
        # (handles "Sep-25" too)
        if "-" in s and len(s) <= 8 and any(ch.isdigit() for ch in s):
            return s

        # otherwise try datetime conversion
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return s  # fallback: keep original if cannot parse

        return dt.strftime("%b-%y")  # Oct-25

    df_deployment['Last Invoice (WCC Issued)'] = df_deployment['Last Invoice (WCC Issued)'].apply(to_mon_yy)

    return df_deployment.fillna("").to_dict(orient="records")
