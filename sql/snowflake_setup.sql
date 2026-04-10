-- ============================================================
-- Snowflake Serving Layer — NOC Network Event Analytics
-- Project: Telecom / NOC Real-Time Streaming Pipeline
-- ============================================================

USE ROLE SYSADMIN;

-- ── Setup ─────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS NOC_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS NOC_DB;
CREATE SCHEMA  IF NOT EXISTS NOC_DB.GOLD;
CREATE SCHEMA  IF NOT EXISTS NOC_DB.AUDIT;

USE WAREHOUSE NOC_WH;
USE DATABASE  NOC_DB;
USE SCHEMA    NOC_DB.GOLD;

-- ── RBAC ──────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS NOC_ANALYST;
CREATE ROLE IF NOT EXISTS NOC_ENGINEER;
GRANT USAGE ON WAREHOUSE NOC_WH       TO ROLE NOC_ANALYST;
GRANT USAGE ON DATABASE  NOC_DB       TO ROLE NOC_ANALYST;
GRANT USAGE ON SCHEMA    NOC_DB.GOLD  TO ROLE NOC_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA NOC_DB.GOLD TO ROLE NOC_ANALYST;

-- ── Tables ────────────────────────────────────────────────────
CREATE OR REPLACE TABLE DEVICE_HEALTH_KPIS (
    device_id               VARCHAR(20)     NOT NULL,
    device_type             VARCHAR(30),
    location                VARCHAR(50),
    location_type           VARCHAR(20),
    event_date              DATE,
    event_hour              INTEGER,
    avg_cpu_pct             FLOAT,
    max_cpu_pct             FLOAT,
    avg_memory_pct          FLOAT,
    max_memory_pct          FLOAT,
    avg_temp_celsius        FLOAT,
    max_temp_celsius        FLOAT,
    sample_count            INTEGER,
    critical_count          INTEGER,
    health_score            FLOAT,
    health_status           VARCHAR(20),
    _gold_ts                TIMESTAMP_NTZ,
    _sf_load_ts             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_date, location)
COMMENT = 'Hourly device health KPIs. One row per device per hour.';

CREATE OR REPLACE TABLE ALERT_SUMMARY (
    event_date              DATE,
    event_type              VARCHAR(30),
    severity                VARCHAR(20),
    location                VARCHAR(50),
    location_type           VARCHAR(20),
    device_type             VARCHAR(30),
    alert_count             INTEGER,
    affected_devices        INTEGER,
    alerts_per_device       FLOAT,
    first_alert_ts          TIMESTAMP_NTZ,
    last_alert_ts           TIMESTAMP_NTZ,
    _gold_ts                TIMESTAMP_NTZ,
    _sf_load_ts             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_date, severity)
COMMENT = 'Daily alert rollup by type, severity, and location.';

CREATE OR REPLACE TABLE NETWORK_RELIABILITY (
    location                VARCHAR(50)     NOT NULL,
    location_type           VARCHAR(20),
    device_type             VARCHAR(30),
    event_date              DATE,
    total_events            INTEGER,
    incident_count          INTEGER,
    avg_packet_loss         FLOAT,
    max_packet_loss         FLOAT,
    avg_bandwidth_util      FLOAT,
    device_count            INTEGER,
    actionable_alerts       INTEGER,
    incident_rate_pct       FLOAT,
    availability_pct        FLOAT,
    sla_met                 BOOLEAN,
    _gold_ts                TIMESTAMP_NTZ,
    _sf_load_ts             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_date, location)
COMMENT = 'Daily network reliability and SLA metrics per location.';

CREATE OR REPLACE TABLE SECURITY_THREATS (
    event_date              DATE,
    alert_type              VARCHAR(60),
    location                VARCHAR(50),
    device_type             VARCHAR(30),
    protocol                VARCHAR(10),
    alert_count             INTEGER,
    total_packets_flagged   BIGINT,
    affected_devices        INTEGER,
    critical_count          INTEGER,
    threat_level            VARCHAR(10),
    _gold_ts                TIMESTAMP_NTZ,
    _sf_load_ts             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_date, threat_level)
COMMENT = 'Daily security threat summary by alert type and location.';

-- ── Analytics Views ───────────────────────────────────────────

-- View 1: NOC executive dashboard
CREATE OR REPLACE VIEW VW_NOC_EXECUTIVE AS
SELECT
    event_date,
    SUM(total_events)                                AS total_events,
    SUM(incident_count)                              AS total_incidents,
    ROUND(AVG(availability_pct), 2)                  AS avg_availability_pct,
    SUM(CASE WHEN sla_met THEN 1 ELSE 0 END)         AS locations_meeting_sla,
    COUNT(DISTINCT location)                         AS total_locations,
    ROUND(SUM(CASE WHEN sla_met THEN 1 ELSE 0 END)
          * 100.0 / COUNT(DISTINCT location), 1)     AS sla_compliance_pct
FROM NETWORK_RELIABILITY
GROUP BY event_date
ORDER BY event_date DESC;

-- View 2: Device health summary
CREATE OR REPLACE VIEW VW_DEVICE_HEALTH_SUMMARY AS
SELECT
    device_id, device_type, location, location_type,
    MAX(event_date)                                  AS latest_date,
    ROUND(AVG(avg_cpu_pct), 1)                       AS avg_cpu,
    ROUND(MAX(max_cpu_pct), 1)                       AS peak_cpu,
    ROUND(AVG(avg_memory_pct), 1)                    AS avg_memory,
    ROUND(AVG(health_score), 1)                      AS avg_health_score,
    SUM(critical_count)                              AS total_critical_events,
    MODE(health_status)                              AS typical_health_status
FROM DEVICE_HEALTH_KPIS
GROUP BY device_id, device_type, location, location_type;

-- View 3: Critical alerts last 24 hours
CREATE OR REPLACE VIEW VW_CRITICAL_ALERTS_24H AS
SELECT
    event_date, event_type, severity, location,
    device_type, alert_count, affected_devices,
    first_alert_ts, last_alert_ts
FROM ALERT_SUMMARY
WHERE severity IN ('CRITICAL','ERROR')
  AND event_date >= DATEADD(day, -1, CURRENT_DATE())
ORDER BY alert_count DESC;

-- View 4: SLA breach locations
CREATE OR REPLACE VIEW VW_SLA_BREACHES AS
SELECT
    location, location_type, event_date,
    device_count, incident_count,
    ROUND(availability_pct, 3)                       AS availability_pct,
    ROUND(99.9 - availability_pct, 3)                AS sla_gap_pct,
    actionable_alerts
FROM NETWORK_RELIABILITY
WHERE sla_met = FALSE
ORDER BY availability_pct ASC;

-- ── Analytics Queries ─────────────────────────────────────────

-- Q1: Current SLA compliance
SELECT * FROM VW_NOC_EXECUTIVE LIMIT 7;

-- Q2: Top 5 most problematic devices
SELECT device_id, location, total_critical_events, avg_health_score, typical_health_status
FROM VW_DEVICE_HEALTH_SUMMARY
ORDER BY total_critical_events DESC LIMIT 5;

-- Q3: Alert trend by day and severity
SELECT
    event_date,
    severity,
    SUM(alert_count)            AS total_alerts,
    SUM(affected_devices)       AS devices_impacted,
    LAG(SUM(alert_count)) OVER (PARTITION BY severity ORDER BY event_date) AS prev_day,
    ROUND((SUM(alert_count) - LAG(SUM(alert_count))
           OVER (PARTITION BY severity ORDER BY event_date))
           * 100.0
           / NULLIF(LAG(SUM(alert_count))
           OVER (PARTITION BY severity ORDER BY event_date), 0), 1) AS day_over_day_pct
FROM ALERT_SUMMARY
GROUP BY event_date, severity
ORDER BY event_date DESC, severity;

-- Q4: Security threat breakdown
SELECT
    alert_type, threat_level,
    SUM(alert_count)            AS total_alerts,
    SUM(total_packets_flagged)  AS packets_flagged,
    SUM(affected_devices)       AS devices_affected,
    SUM(critical_count)         AS critical_alerts
FROM SECURITY_THREATS
GROUP BY alert_type, threat_level
ORDER BY total_alerts DESC;

-- Q5: Bandwidth utilization by location
SELECT
    location, location_type,
    ROUND(AVG(avg_bandwidth_util), 1)   AS avg_bandwidth_pct,
    ROUND(AVG(avg_packet_loss), 3)      AS avg_packet_loss_pct,
    COUNT(DISTINCT event_date)          AS days_monitored
FROM NETWORK_RELIABILITY
GROUP BY location, location_type
ORDER BY avg_bandwidth_pct DESC;
