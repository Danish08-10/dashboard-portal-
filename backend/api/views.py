from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .utils2 import section1,section2,section4,section5,section6,section7,section8,section9,df_sect1,df_sect2,df_sect8,section10,df_invoices_mrc,df_invoice,section11,start_date,end_date
from datetime import datetime
from .Sla_Utils import compute_sla_working_hours
from .Sla_breach_utils import apply_targets_and_breaches, sla_kpis_and_breached_table
import os
import tempfile
from django.http import FileResponse, HttpResponseServerError
from django.views.decorators.http import require_GET
from playwright.sync_api import sync_playwright

report_period = f"{start_date.strftime('%d %b %Y')} – {end_date.strftime('%d %b %Y')}"

@api_view(['GET'])
def Summary_view(Request):
    total_projects, completed_projects, total_pending_projects, inprogress_projects, pending_customer_end_count,not_feasible,rejected=section1(df_sect1)
    no_of_awaited,no_of_signed=section4(df_sect1)
    missing,filled=section5(df_sect1)
    not_handed_over, handed_over,not_handed_over_table=section6(df_sect1)
    total, closed, not_closed, not_closed_table=section7(df_sect2)
    total_vendors,total_links,wireless_links,wired_links=section10(df_invoices_mrc)

    return Response({
        'report_date':report_period,
        'total_projects':total_projects,
        'completed_projects':completed_projects,
        'inprogress':inprogress_projects,
        'Pending_customer_end':pending_customer_end_count,
        'not_feasible':not_feasible,
        'rejected':rejected,
        'Caf_Awaited':no_of_awaited,
        'Missing_Unique_Id':missing,
        'Filled_Unique_Id':filled,
        'Not_Handed_over_count':not_handed_over,
        'handed_over':handed_over,
        'Total_CSP':total,
        'Closed':closed,
        'not_closed':not_closed,
        "total_vendors": total_vendors,
        "total_links": total_links,
        "wireless_links": wireless_links,
        "wired_links": wired_links,
        "Signed_CAF":no_of_signed
    })

@api_view(['GET'])
def Table_view(Request):
    table_row=section2(df_sect1)
    no_of_awaited,CAF_Signed=section4(df_sect1)
    Caf_row=1
    not_handed_over, handed_over,not_handed_over_table=section6(df_sect1)
    total, closed, not_closed, not_closed_table=section7(df_sect2)

    return Response({
        'inprogress_table':table_row,
        'CAF_Table':Caf_row,
        'Not_handed_over':not_handed_over_table,
        'Not_Closed_Table':not_closed_table
    })

@api_view(['GET'])
def Chart(Request):
    Progress_data=section8(df_sect8)
    return Response(Progress_data)

@api_view(['GET'])
def Chart2(Request):
    PIE_chart_data=section9(df_sect1)
    return Response(PIE_chart_data)

@api_view(['GET'])
def Invoices(Request):
    invoices_status=section11(df_invoice)
    return Response(invoices_status)

@api_view(["GET"])
def sla_kpi_dashboard(request):
    df_final = compute_sla_working_hours(df_sect2)
    df_final = apply_targets_and_breaches(df_final)
    payload = sla_kpis_and_breached_table(df_final)
    return Response(payload)


@require_GET
def export_deployment_report_pdf(request):
    """
    Generates PDF from React report page using headless Chromium.
    React URL must be accessible from server.
    """
    report_url = request.GET.get("url") or "http://localhost:5173/report"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # 1) Load the report page
            page.goto(report_url, wait_until="domcontentloaded", timeout=120000)

            # 2) Wait for report layout
            page.wait_for_selector(".report-page", timeout=120000)

            # 3) Wait for Recharts to mount
            page.wait_for_selector(".recharts-wrapper", timeout=120000)

            # 4) Wait until charts have non-zero size (avoid blank charts)
            page.wait_for_function(
                """
                () => {
                  const els = document.querySelectorAll('.recharts-wrapper');
                  if (!els.length) return false;
                  return Array.from(els).every(el => {
                    const r = el.getBoundingClientRect();
                    return r.width > 50 && r.height > 50;
                  });
                }
                """,
                timeout=120000
            )

            # 5) Small buffer + ensure "screen" media rendering
            page.wait_for_timeout(500)
            page.emulate_media(media="print")

            # 6) Create temp PDF
            fd, pdf_path = tempfile.mkstemp(suffix=".pdf")
            os.close(fd)

            page.pdf(
            path=pdf_path,
            format="A4",
            landscape=True,
            print_background=True,

            # Enable header/footer
            display_header_footer=True,

            margin={"top": "12mm", "right": "10mm", "bottom": "16mm", "left": "10mm"},

            header_template="""
            <div style="font-size:10px;width:100%;text-align:right;padding-right:10px;color:#6b7280;">
                Deployment Tracker Report
            </div>
            """,

            footer_template="""
            <div style="
                font-size:10px;
                width:100%;
                text-align:center;
                border-top:1px solid #e5e7eb;
                padding-top:6px;
                color:#6b7280;">
                Page <span class="pageNumber"></span> of <span class="totalPages"></span>
            </div>
            """
        )
            browser.close()

        return FileResponse(open(pdf_path, "rb"), as_attachment=True, filename="Deployment_Report.pdf")

    except Exception as e:
        return HttpResponseServerError(f"PDF generation failed: {str(e)}")
