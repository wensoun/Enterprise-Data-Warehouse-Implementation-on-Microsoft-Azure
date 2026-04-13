# Troubleshooting Guide

## Data Warehouse Implementation on Azure with CDC

---

## Table of Contents

1. [CDC Issues](#1-cdc-issues)
2. [SHIR Connection Issues](#2-shir-connection-issues)
3. [ADF Pipeline Issues](#3-adf-pipeline-issues)
4. [Synapse Performance Issues](#4-synapse-performance-issues)
5. [Power BI Issues](#5-power-bi-issues)
6. [Data Quality Issues](#6-data-quality-issues)
7. [Common Error Codes](#7-common-error-codes)

---

## 1. CDC Issues

### 1.1 CDC Not Capturing Changes

**Symptoms:**
- Change tables show no new records
- SQL Agent CDC job not running

**Solutions:**

```sql
-- Check if CDC is enabled
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'AdventureWorks2019';

-- Check CDC jobs status
EXEC sys.sp_cdc_help_jobs;

-- Restart CDC capture job
EXEC sys.sp_cdc_start_job @job_type = 'capture';

-- Verify CDC is working by testing
INSERT INTO test_table VALUES ('test');
SELECT * FROM cdc.test_table_CT;