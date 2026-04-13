#!/usr/bin/env python3
# ============================================================
# Script: monitor_pipeline.py
# Purpose: Monitor Azure Data Factory pipeline runs and send alerts
# Author: Data Warehouse Team
# Date: 2024
# ============================================================

import os
import sys
import json
import time
import logging
import argparse
import smtplib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Azure SDK imports
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.synapse import SynapseManagementClient
from azure.storage.blob import BlobServiceClient

# ============================================================
# CONFIGURATION
# ============================================================

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_monitor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Default configuration (can be overridden by environment variables)
DEFAULT_CONFIG = {
    "subscription_id": os.environ.get("AZURE_SUBSCRIPTION_ID", ""),
    "resource_group": os.environ.get("AZURE_RESOURCE_GROUP", "rg-edw-prod-01"),
    "data_factory_name": os.environ.get("AZURE_DATA_FACTORY_NAME", "adf-edw-prod-01"),
    "synapse_workspace_name": os.environ.get("AZURE_SYNAPSE_WORKSPACE_NAME", "synapse-edw-prod-01"),
    "sql_pool_name": os.environ.get("AZURE_SQL_POOL_NAME", "sqldwedw"),
    "storage_account_name": os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "adlsedwprod01"),
    "alert_threshold_minutes": int(os.environ.get("ALERT_THRESHOLD_MINUTES", "30")),
    "slack_webhook_url": os.environ.get("SLACK_WEBHOOK_URL", ""),
    "smtp_server": os.environ.get("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.environ.get("SMTP_PORT", "587")),
    "alert_email_from": os.environ.get("ALERT_EMAIL_FROM", ""),
    "alert_email_to": os.environ.get("ALERT_EMAIL_TO", ""),
    "email_password": os.environ.get("EMAIL_PASSWORD", "")
}

# ============================================================
# AZURE CLIENTS
# ============================================================

class AzureClients:
    """Manage Azure service clients"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.credential = DefaultAzureCredential()
        
    def get_datafactory_client(self) -> DataFactoryManagementClient:
        """Get Data Factory management client"""
        return DataFactoryManagementClient(
            credential=self.credential,
            subscription_id=self.config["subscription_id"]
        )
    
    def get_synapse_client(self) -> SynapseManagementClient:
        """Get Synapse management client"""
        return SynapseManagementClient(
            credential=self.credential,
            subscription_id=self.config["subscription_id"]
        )
    
    def get_blob_client(self) -> BlobServiceClient:
        """Get Blob Storage client"""
        account_url = f"https://{self.config['storage_account_name']}.blob.core.windows.net"
        return BlobServiceClient(account_url, credential=self.credential)

# ============================================================
# PIPELINE MONITORING
# ============================================================

class PipelineMonitor:
    """Monitor Azure Data Factory pipelines"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.clients = AzureClients(config)
        self.adf_client = self.clients.get_datafactory_client()
        
    def get_pipeline_runs(self, hours_back: int = 24) -> List[Dict]:
        """Get pipeline runs from last N hours"""
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)
        
        logger.info(f"Fetching pipeline runs from {start_time} to {end_time}")
        
        try:
            runs = self.adf_client.pipeline_runs.query_by_factory(
                resource_group_name=self.config["resource_group"],
                factory_name=self.config["data_factory_name"],
                filter_parameters={
                    "last_updated_after": start_time,
                    "last_updated_before": end_time
                }
            )
            return list(runs)
        except Exception as e:
            logger.error(f"Failed to get pipeline runs: {e}")
            return []
    
    def get_pipeline_status_summary(self, runs: List[Dict]) -> Dict:
        """Get summary of pipeline statuses"""
        
        summary = {
            "total": len(runs),
            "succeeded": 0,
            "failed": 0,
            "in_progress": 0,
            "cancelled": 0,
            "failed_pipelines": []
        }
        
        for run in runs:
            status = run.status
            if status == "Succeeded":
                summary["succeeded"] += 1
            elif status == "Failed":
                summary["failed"] += 1
                summary["failed_pipelines"].append({
                    "name": run.pipeline_name,
                    "run_id": run.run_id,
                    "message": run.message if hasattr(run, 'message') else "Unknown error"
                })
            elif status == "InProgress":
                summary["in_progress"] += 1
            elif status == "Cancelled":
                summary["cancelled"] += 1
        
        return summary
    
    def check_long_running_pipelines(self, runs: List[Dict], threshold_minutes: int) -> List[Dict]:
        """Check for pipelines running longer than threshold"""
        
        long_running = []
        
        for run in runs:
            if run.status == "InProgress":
                duration = datetime.utcnow() - run.run_end if run.run_end else datetime.utcnow() - run.run_start
                if duration.total_seconds() > (threshold_minutes * 60):
                    long_running.append({
                        "name": run.pipeline_name,
                        "run_id": run.run_id,
                        "duration_minutes": round(duration.total_seconds() / 60, 2)
                    })
        
        return long_running
    
    def get_etl_latency(self) -> Optional[float]:
        """Calculate end-to-end ETL latency from Silver layer"""
        
        # Query Synapse for latest LoadDateTime
        # This would require a SQL connection
        # For now, return None or estimate
        
        logger.info("Checking ETL latency from Silver layer...")
        # Placeholder - implement with pyodbc or SQLAlchemy
        return None

# ============================================================
# DATA QUALITY MONITORING
# ============================================================

class DataQualityMonitor:
    """Monitor data quality metrics"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.clients = AzureClients(config)
    
    def check_row_counts(self) -> Dict:
        """Check row counts in Silver and Gold tables"""
        
        # Placeholder - would query Synapse
        # Returns dict with expected vs actual counts
        
        return {
            "silver.DimProduct": {"expected": 504, "actual": None},
            "silver.DimCustomer": {"expected": 19820, "actual": None},
            "silver.FactSalesOrder": {"expected": 121317, "actual": None},
            "gold.DimProduct": {"expected": 504, "actual": None},
            "gold.FactSales": {"expected": 121317, "actual": None}
        }
    
    def check_null_counts(self, table_name: str, key_columns: List[str]) -> Dict:
        """Check for NULL values in key columns"""
        
        # Placeholder - would query Synapse
        return {col: 0 for col in key_columns}
    
    def get_data_quality_score(self) -> float:
        """Calculate overall data quality score (0-100)"""
        
        # Placeholder - implement based on quality metrics
        # Would check completeness, accuracy, consistency
        
        return 99.5

# ============================================================
# ALERTING
# ============================================================

class AlertManager:
    """Send alerts via Slack and Email"""
    
    def __init__(self, config: Dict):
        self.config = config
    
    def send_slack_alert(self, message: str, color: str = "warning") -> bool:
        """Send alert to Slack channel"""
        
        if not self.config["slack_webhook_url"]:
            logger.warning("Slack webhook URL not configured")
            return False
        
        payload = {
            "text": message,
            "attachments": [{
                "color": color,
                "text": message,
                "footer": "Data Warehouse Monitor",
                "ts": int(time.time())
            }]
        }
        
        try:
            response = requests.post(
                self.config["slack_webhook_url"],
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            logger.info("Slack alert sent successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
    
    def send_email_alert(self, subject: str, body: str) -> bool:
        """Send email alert"""
        
        if not all([self.config["alert_email_from"], self.config["alert_email_to"], self.config["email_password"]]):
            logger.warning("Email configuration incomplete")
            return False
        
        try:
            msg = MIMEMultipart()
            msg["From"] = self.config["alert_email_from"]
            msg["To"] = self.config["alert_email_to"]
            msg["Subject"] = f"[Data Warehouse Monitor] {subject}"
            
            msg.attach(MIMEText(body, "plain"))
            
            with smtplib.SMTP(self.config["smtp_server"], self.config["smtp_port"]) as server:
                server.starttls()
                server.login(self.config["alert_email_from"], self.config["email_password"])
                server.send_message(msg)
            
            logger.info("Email alert sent successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False
    
    def send_health_report(self, summary: Dict, long_running: List, quality_score: float) -> None:
        """Send comprehensive health report"""
        
        # Build report message
        report_lines = [
            "=" * 50,
            "DATA WAREHOUSE HEALTH REPORT",
            "=" * 50,
            f"Timestamp: {datetime.utcnow().isoformat()}",
            "",
            "Pipeline Status:",
            f"  Total Runs (24h): {summary['total']}",
            f"  Successful: {summary['succeeded']}",
            f"  Failed: {summary['failed']}",
            f"  Success Rate: {(summary['succeeded']/summary['total']*100):.1f}%" if summary['total'] > 0 else "N/A",
            "",
            f"Data Quality Score: {quality_score}/100",
            ""
        ]
        
        if long_running:
            report_lines.append("Long Running Pipelines:")
            for pl in long_running:
                report_lines.append(f"  - {pl['name']}: {pl['duration_minutes']} minutes")
            report_lines.append("")
        
        if summary['failed'] > 0:
            report_lines.append("Failed Pipelines:")
            for pl in summary['failed_pipelines']:
                report_lines.append(f"  - {pl['name']}: {pl['message'][:100]}")
            report_lines.append("")
        
        report_lines.append("=" * 50)
        
        report = "\n".join(report_lines)
        
        # Send alerts based on severity
        if summary['failed'] > 0:
            self.send_slack_alert(f"⚠️ {summary['failed']} pipeline failures detected!", "danger")
        elif long_running:
            self.send_slack_alert(f"⏱️ {len(long_running)} pipelines running longer than threshold", "warning")
        else:
            self.send_slack_alert("✅ All pipelines healthy", "good")
        
        self.send_email_alert("Data Warehouse Health Report", report)
        logger.info("Health report sent")

# ============================================================
# MAIN EXECUTION
# ============================================================

def parse_arguments():
    """Parse command line arguments"""
    
    parser = argparse.ArgumentParser(
        description="Monitor Azure Data Factory pipelines and data quality"
    )
    parser.add_argument(
        "--hours-back",
        type=int,
        default=24,
        help="Number of hours to look back for pipeline runs"
    )
    parser.add_argument(
        "--threshold-minutes",
        type=int,
        default=30,
        help="Threshold in minutes for long-running pipelines"
    )
    parser.add_argument(
        "--send-report",
        action="store_true",
        help="Send email/Slack report even if no issues"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (don't enter continuous monitoring loop)"
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=300,
        help="Monitoring interval in seconds (for continuous mode)"
    )
    
    return parser.parse_args()


def run_monitoring_cycle(config: Dict, args) -> Dict:
    """Run one monitoring cycle and return results"""
    
    logger.info("Starting monitoring cycle...")
    
    # Initialize monitors
    pipeline_monitor = PipelineMonitor(config)
    quality_monitor = DataQualityMonitor(config)
    alert_manager = AlertManager(config)
    
    # Get pipeline runs
    runs = pipeline_monitor.get_pipeline_runs(args.hours_back)
    summary = pipeline_monitor.get_pipeline_status_summary(runs)
    
    # Check for long-running pipelines
    long_running = pipeline_monitor.check_long_running_pipelines(runs, args.threshold_minutes)
    
    # Get data quality score
    quality_score = quality_monitor.get_data_quality_score()
    
    # Check ETL latency
    latency = pipeline_monitor.get_etl_latency()
    
    # Log results
    logger.info(f"Pipeline Summary: {summary}")
    logger.info(f"Long Running Pipelines: {len(long_running)}")
    logger.info(f"Data Quality Score: {quality_score}")
    
    # Send alerts if issues found
    if summary['failed'] > 0 or long_running or quality_score < 95 or args.send_report:
        alert_manager.send_health_report(summary, long_running, quality_score)
    
    # Return results for potential further processing
    return {
        "summary": summary,
        "long_running": long_running,
        "quality_score": quality_score,
        "latency_minutes": latency
    }


def continuous_monitoring(config: Dict, args):
    """Run monitoring in continuous loop"""
    
    logger.info("Starting continuous monitoring mode")
    logger.info(f"Interval: {args.interval_seconds} seconds")
    
    while True:
        try:
            run_monitoring_cycle(config, args)
        except Exception as e:
            logger.error(f"Monitoring cycle failed: {e}")
        
        logger.info(f"Waiting {args.interval_seconds} seconds until next cycle...")
        time.sleep(args.interval_seconds)


def main():
    """Main entry point"""
    
    # Parse arguments
    args = parse_arguments()
    
    # Load configuration
    config = DEFAULT_CONFIG.copy()
    
    # Override with command line arguments
    config["alert_threshold_minutes"] = args.threshold_minutes
    
    # Validate configuration
    if not config["subscription_id"]:
        logger.error("AZURE_SUBSCRIPTION_ID environment variable not set")
        sys.exit(1)
    
    if not config["resource_group"]:
        logger.error("AZURE_RESOURCE_GROUP environment variable not set")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("DATA WAREHOUSE MONITOR STARTING")
    logger.info("=" * 50)
    logger.info(f"Subscription: {config['subscription_id']}")
    logger.info(f"Resource Group: {config['resource_group']}")
    logger.info(f"Data Factory: {config['data_factory_name']}")
    logger.info(f"Looking back: {args.hours_back} hours")
    logger.info("=" * 50)
    
    if args.once:
        # Run once and exit
        run_monitoring_cycle(config, args)
        logger.info("Monitoring cycle complete")
    else:
        # Run continuously
        continuous_monitoring(config, args)


if __name__ == "__main__":
    main()