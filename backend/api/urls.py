from django.urls import path
from .views import Summary_view,Table_view,Chart,Chart2,Invoices,export_deployment_report_pdf,sla_kpi_dashboard

urlpatterns = [
    path('dashboard/', Summary_view),
    path('Table/',Table_view),
    path('ProgressData',Chart),
    path('PieChart',Chart2),
    path('InvoiceStatus',Invoices),
    path("export-pdf/", export_deployment_report_pdf, name="export_deployment_report_pdf"),
    path("sla-kpis/", sla_kpi_dashboard)
]
