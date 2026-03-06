from django.test import TestCase
import pandas as pd
import os

from django.conf import settings

from .Sla_Utils import compute_sla_working_hours
from .utils2 import df_sect2  # your filtered df for report
from .Sla_breach_utils import apply_targets_and_breaches


class SLABreachTests(TestCase):

    def test_breach_cases_export_and_print(self):
        # 1) Compute SLA (Working Hours)
        df_final = compute_sla_working_hours(df_sect2)

        self.assertGreater(len(df_final), 0)
        self.assertIn("Working Hours", df_final.columns)

        # 2) Apply targets + breach flag
        df_with_breaches = apply_targets_and_breaches(df_final)

        # sanity
        self.assertIn("SLA_Target", df_with_breaches.columns)
        self.assertIn("SLA_Unit", df_with_breaches.columns)
        self.assertIn("SLA_Breached", df_with_breaches.columns)

        # 3) Filter breached rows
        breached_df = df_with_breaches[df_with_breaches["SLA_Breached"] == True].copy()

        # 4) Print summary + top breakdowns
        print("\n================ SLA BREACH SUMMARY ================")
        print("Total rows:", len(df_with_breaches))
        print("Breached rows:", len(breached_df))

        if len(breached_df) > 0:
            print("\nBreaches by Category:")
            print(breached_df["Category"].astype(str).value_counts().head(20))

            if "Wired/Wireless" in breached_df.columns:
                print("\nBreaches by Wired/Wireless:")
                print(breached_df["Wired/Wireless"].astype(str).value_counts())

            if "Service" in breached_df.columns:
                print("\nBreaches by Service:")
                print(breached_df["Service"].astype(str).value_counts().head(20))

        # 5) Export details to Excel (only breached links)
        export_cols = [
            "unique_id",
            "ESS Client",
            "Category",
            "Service",
            "Wired/Wireless",
            "Last Mile vendor ",
            "Working Hours",
            "SLA_Target",
            "SLA_Unit",
            "Remarks",
            "Location",
            "Date of go ahead",
            "Time of go ahead",
            "Date of completion at cmpak end work",
            "Deployment Time",
            "WORK ORDER APPROVAL",
            "Date/Time of go ahead",
            "Date/Time of Deployment",
        ]
        export_cols = [c for c in export_cols if c in breached_df.columns]

        breached_export = breached_df[export_cols].copy()

        # Optional: sort worst breaches first
        breached_export["Working Hours"] = pd.to_numeric(breached_export["Working Hours"], errors="coerce")
        breached_export["SLA_Target"] = pd.to_numeric(breached_export["SLA_Target"], errors="coerce")
        breached_export["Breach_Amount"] = breached_export["Working Hours"] - breached_export["SLA_Target"]
        breached_export = breached_export.sort_values("Breach_Amount", ascending=False)

        # Write file to project root (or any folder you prefer)
        out_path = os.path.join(settings.BASE_DIR, "SLA_Breached_Links.xlsx")
        breached_export.to_excel(out_path, index=False)

        print("\n✅ Exported breached links file:", out_path)
        print("====================================================\n")

        # 6) Assert just to confirm test meaningfully ran
        # (We don't force minimum breaches; your data can have zero breaches)
        self.assertTrue(True)
