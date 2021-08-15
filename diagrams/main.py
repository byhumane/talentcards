from diagrams import Cluster, Diagram
from diagrams.custom import Custom
from diagrams.gcp.analytics import Bigquery
from diagrams.gcp.compute import GCF
from diagrams.gcp.devtools import Scheduler
from diagrams.gcp.storage import GCS

with Diagram(filename="humane_architecture"):
    with Cluster("Data Sources"):
        talentcards = Custom("TalentCards API", "icons/talentcards.png")

    cloud_functions_lz = GCF("Cloud functions \n [send data to Cloud Storage]")

    with Cluster("Data Lake"):
        landing_zone = GCS("Landing zone")

    cloud_functions_bq = GCF("Cloud functions \n [send data to BigQuery]")

    with Cluster("Data Warehouse"):
        bigquery = Bigquery("BigQuery")

    with Cluster("Delivery Layer"):
        dashboard = Custom("Dashboards", "icons/googledatastudio-logo.png")

    schedulers = Scheduler("Schedulers")

    talentcards >> cloud_functions_lz >> landing_zone >> cloud_functions_bq >> bigquery >> dashboard
    schedulers >> [cloud_functions_lz, cloud_functions_bq]
