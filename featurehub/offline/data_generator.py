"""Synthetic Historical Data Generator for PostgreSQL Offline Store.
Generates realistic multi-entity timelines with legitimate and fraudulent transaction scenarios.
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple


def generate_historical_dataset(
    num_customers: int = 50,
    num_merchants: int = 30,
    num_devices: int = 60,
    num_transactions: int = 1500,
    fraud_rate: float = 0.08,
    seed: int = 42,
) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Generates synthetic PostgreSQL historical tables:
    1. events_df: Observation dataframe of transaction events with fraud labels.
    2. feature_tables: Historical feature tables per entity (customer, merchant, device).
    """
    rng = np.random.default_rng(seed)
    base_time = datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)

    customer_ids = [f"cust_{i:04d}" for i in range(1, num_customers + 1)]
    merchant_ids = [f"merch_{j:04d}" for j in range(1, num_merchants + 1)]
    device_ids = [f"dev_{k:04d}" for k in range(1, num_devices + 1)]

    # ---------------------------------------------------------
    # 1. Customer Feature Timeline (snapshots every 6 hours)
    # ---------------------------------------------------------
    customer_rows = []
    for cid in customer_ids:
        # Base baseline profile
        base_avg_spend = rng.uniform(25.0, 150.0)
        base_tenure = rng.uniform(30.0, 1200.0)
        base_credit = int(rng.integers(580, 830))
        base_risk_tier = rng.choice([1, 2, 3], p=[0.7, 0.25, 0.05])

        for step in range(10):  # 10 snapshot points across 2.5 days
            snapshot_time = base_time + timedelta(hours=step * 6)
            tx_count_24h = int(rng.poisson(lam=4))
            spend_24h = float(tx_count_24h * base_avg_spend * rng.uniform(0.8, 1.2))

            customer_rows.append({
                "customer_id": cid,
                "feature_timestamp": snapshot_time,
                "customer_tx_count_1h": int(rng.poisson(lam=0.5)),
                "customer_tx_count_24h": tx_count_24h,
                "customer_tx_count_7d": int(tx_count_24h * 5.5),
                "customer_tx_count_30d": int(tx_count_24h * 22),
                "customer_spend_amount_1h": float(rng.uniform(0, 45)),
                "customer_spend_amount_24h": spend_24h,
                "customer_spend_amount_7d": float(spend_24h * 5.2),
                "customer_spend_amount_30d": float(spend_24h * 21.0),
                "customer_avg_spend_amount_30d": base_avg_spend,
                "customer_stddev_spend_amount_30d": float(base_avg_spend * 0.4),
                "customer_max_spend_amount_30d": float(base_avg_spend * 3.5),
                "customer_distinct_merchants_24h": max(1, int(tx_count_24h * 0.7)),
                "customer_distinct_merchants_7d": max(2, int(tx_count_24h * 3.0)),
                "customer_distinct_categories_7d": int(rng.integers(1, 6)),
                "customer_distinct_countries_7d": int(rng.choice([1, 2, 3], p=[0.92, 0.06, 0.02])),
                "customer_distinct_cities_24h": int(rng.choice([1, 2], p=[0.94, 0.06])),
                "customer_failed_tx_count_24h": int(rng.choice([0, 1, 3], p=[0.88, 0.09, 0.03])),
                "customer_failed_pin_count_24h": int(rng.choice([0, 1], p=[0.97, 0.03])),
                "customer_refund_count_30d": int(rng.poisson(lam=0.3)),
                "customer_refund_ratio_30d": float(rng.uniform(0.0, 0.04)),
                "customer_chargeback_count_lifetime": int(rng.choice([0, 1], p=[0.95, 0.05])),
                "customer_chargeback_rate_lifetime": float(rng.uniform(0.0, 0.01)),
                "customer_days_since_first_tx": base_tenure + (step * 0.25),
                "customer_days_since_last_tx": float(rng.uniform(0.1, 4.0)),
                "customer_days_since_password_reset": float(rng.uniform(5.0, 180.0)),
                "customer_days_since_phone_change": float(rng.uniform(20.0, 365.0)),
                "customer_days_since_email_change": float(rng.uniform(40.0, 500.0)),
                "customer_kyc_verification_level": int(rng.choice([2, 3], p=[0.4, 0.6])),
                "customer_credit_score": base_credit,
                "customer_current_balance": float(rng.uniform(500.0, 12000.0)),
                "customer_credit_utilization_ratio": float(rng.uniform(0.05, 0.65)),
                "customer_overdraft_count_90d": int(rng.choice([0, 1], p=[0.92, 0.08])),
                "customer_is_vip": bool(rng.choice([True, False], p=[0.1, 0.9])),
                "customer_is_politically_exposed": False,
                "customer_risk_tier": base_risk_tier,
            })
    customer_df = pd.DataFrame(customer_rows)

    # ---------------------------------------------------------
    # 2. Merchant Feature Timeline
    # ---------------------------------------------------------
    merchant_rows = []
    for mid in merchant_ids:
        base_ticket = rng.uniform(20.0, 220.0)
        base_fraud_rate = float(rng.uniform(0.0005, 0.008))
        base_risk_score = float(rng.uniform(0.05, 0.35))
        mcc_risk = float(rng.uniform(0.1, 0.5))

        for step in range(10):
            snapshot_time = base_time + timedelta(hours=step * 6)
            tx_24h = int(rng.integers(50, 600))
            vol_24h = float(tx_24h * base_ticket)

            merchant_rows.append({
                "merchant_id": mid,
                "feature_timestamp": snapshot_time,
                "merchant_tx_count_1h": int(tx_24h / 24 * rng.uniform(0.7, 1.4)),
                "merchant_tx_count_24h": tx_24h,
                "merchant_tx_count_7d": int(tx_24h * 6.8),
                "merchant_tx_volume_24h": vol_24h,
                "merchant_tx_volume_7d": float(vol_24h * 6.5),
                "merchant_avg_tx_amount_30d": base_ticket,
                "merchant_stddev_tx_amount_30d": float(base_ticket * 0.45),
                "merchant_max_single_tx_amount_30d": float(base_ticket * 4.2),
                "merchant_distinct_cards_24h": int(tx_24h * 0.9),
                "merchant_distinct_customers_7d": int(tx_24h * 5.0),
                "merchant_fraud_reports_count_30d": int(rng.poisson(lam=0.8)),
                "merchant_fraud_rate_30d": base_fraud_rate,
                "merchant_chargeback_count_30d": int(rng.poisson(lam=1.2)),
                "merchant_chargeback_ratio_30d": float(rng.uniform(0.001, 0.006)),
                "merchant_refund_count_30d": int(tx_24h * 0.04),
                "merchant_refund_rate_30d": float(rng.uniform(0.015, 0.04)),
                "merchant_dispute_win_rate_90d": float(rng.uniform(0.55, 0.85)),
                "merchant_risk_score": base_risk_score,
                "merchant_category_risk_index": mcc_risk,
                "merchant_tenure_days": float(rng.uniform(100.0, 900.0)),
                "merchant_is_high_risk_mcc": bool(rng.choice([True, False], p=[0.08, 0.92])),
                "merchant_is_card_not_present_only": bool(rng.choice([True, False], p=[0.6, 0.4])),
                "merchant_international_tx_ratio_30d": float(rng.uniform(0.02, 0.15)),
                "merchant_midnight_tx_ratio_7d": float(rng.uniform(0.01, 0.08)),
                "merchant_terminal_count": int(rng.integers(1, 10)),
                "merchant_manual_entry_ratio_30d": float(rng.uniform(0.005, 0.03)),
                "merchant_daily_volume_limit": 100000.0,
                "merchant_daily_volume_utilization": float(vol_24h / 100000.0),
                "merchant_suspicious_velocity_flag": False,
                "merchant_kyc_status": 3,
            })
    merchant_df = pd.DataFrame(merchant_rows)

    # ---------------------------------------------------------
    # 3. Device Feature Timeline
    # ---------------------------------------------------------
    device_rows = []
    for did in device_ids:
        is_bad_device = bool(rng.choice([True, False], p=[0.1, 0.9]))
        trust = float(rng.uniform(0.05, 0.35)) if is_bad_device else float(rng.uniform(0.80, 0.98))
        ip_rep = float(rng.uniform(65.0, 95.0)) if is_bad_device else float(rng.uniform(2.0, 20.0))

        for step in range(10):
            snapshot_time = base_time + timedelta(hours=step * 6)
            device_rows.append({
                "device_id": did,
                "feature_timestamp": snapshot_time,
                "device_tx_count_1h": int(rng.poisson(lam=3 if is_bad_device else 0.4)),
                "device_tx_count_24h": int(rng.poisson(lam=12 if is_bad_device else 2.5)),
                "device_distinct_cards_24h": int(rng.integers(4, 9)) if is_bad_device else 1,
                "device_distinct_customers_7d": int(rng.integers(3, 7)) if is_bad_device else 1,
                "device_is_emulator": is_bad_device and bool(rng.choice([True, False], p=[0.5, 0.5])),
                "device_is_rooted_jailbroken": is_bad_device and bool(rng.choice([True, False], p=[0.4, 0.6])),
                "device_is_proxy_or_vpn": is_bad_device or bool(rng.choice([True, False], p=[0.1, 0.9])),
                "device_is_tor_exit_node": is_bad_device and bool(rng.choice([True, False], p=[0.2, 0.8])),
                "device_is_datacenter_ip": is_bad_device and bool(rng.choice([True, False], p=[0.3, 0.7])),
                "device_ip_reputation_score": ip_rep,
                "device_ip_country_mismatch": is_bad_device and bool(rng.choice([True, False], p=[0.4, 0.6])),
                "device_ip_city_distance_km": float(rng.uniform(200.0, 1500.0) if is_bad_device else rng.uniform(2.0, 25.0)),
                "device_os_version_outdated": bool(rng.choice([True, False], p=[0.15, 0.85])),
                "device_browser_headless_flag": is_bad_device and bool(rng.choice([True, False], p=[0.35, 0.65])),
                "device_canvas_fingerprint_hash": f"hash_{hash(did) % 100000:06d}",
                "device_timezone_offset_mismatch": is_bad_device and bool(rng.choice([True, False], p=[0.3, 0.7])),
                "device_battery_level": float(rng.uniform(0.15, 0.98)),
                "device_battery_charging": bool(rng.choice([True, False])),
                "device_screen_resolution_anomalous": is_bad_device and bool(rng.choice([True, False], p=[0.4, 0.6])),
                "device_touch_event_missing": is_bad_device and bool(rng.choice([True, False], p=[0.25, 0.75])),
                "device_connection_type_cellular": bool(rng.choice([True, False], p=[0.6, 0.4])),
                "device_connection_type_wifi": bool(rng.choice([True, False], p=[0.4, 0.6])),
                "device_tcp_rtt_ms": int(rng.integers(180, 450) if is_bad_device else rng.integers(25, 75)),
                "device_age_days": float(rng.uniform(1.0, 15.0) if is_bad_device else rng.uniform(60.0, 600.0)),
                "device_trust_score": trust,
            })
    device_df = pd.DataFrame(device_rows)

    # ---------------------------------------------------------
    # 4. Observation Events: Transactions Table
    # ---------------------------------------------------------
    event_rows = []
    # Events occur between base_time + 6 hours and base_time + 54 hours
    for idx in range(num_transactions):
        tx_id = f"tx_{idx+1:06d}"
        cid = rng.choice(customer_ids)
        mid = rng.choice(merchant_ids)
        did = rng.choice(device_ids)

        # Event occurs in interval [6h, 54h] after base_time
        offset_seconds = rng.uniform(6 * 3600 + 60, 54 * 3600)
        event_time = base_time + timedelta(seconds=offset_seconds)

        # Decide if this transaction is fraudulent
        is_fraud = bool(rng.choice([True, False], p=[fraud_rate, 1.0 - fraud_rate]))

        if is_fraud:
            amount = float(rng.uniform(350.0, 2400.0))
            is_night = bool(rng.choice([True, False], p=[0.6, 0.4]))
            is_impossible_travel = bool(rng.choice([True, False], p=[0.4, 0.6]))
            is_foreign = bool(rng.choice([True, False], p=[0.5, 0.5]))
            cvv_matched = bool(rng.choice([True, False], p=[0.6, 0.4]))
            threeds_auth = bool(rng.choice([True, False], p=[0.2, 0.8]))
            split_bill = bool(rng.choice([True, False], p=[0.35, 0.65]))
            round_amt = bool(rng.choice([True, False], p=[0.4, 0.6]))
            speed = float(rng.uniform(650.0, 1200.0)) if is_impossible_travel else float(rng.uniform(40.0, 180.0))
        else:
            amount = float(rng.exponential(scale=55.0) + 5.0)
            is_night = bool(rng.choice([True, False], p=[0.1, 0.9]))
            is_impossible_travel = False
            is_foreign = bool(rng.choice([True, False], p=[0.05, 0.95]))
            cvv_matched = True
            threeds_auth = True
            split_bill = False
            round_amt = bool(rng.choice([True, False], p=[0.05, 0.95]))
            speed = float(rng.uniform(5.0, 80.0))

        hour = event_time.hour
        dow = event_time.weekday()

        event_rows.append({
            "transaction_id": tx_id,
            "customer_id": cid,
            "merchant_id": mid,
            "device_id": did,
            "timestamp": event_time,
            "tx_amount": amount,
            "tx_amount_to_customer_avg_ratio": amount / 60.0,
            "tx_amount_to_customer_max_ratio": amount / 350.0,
            "tx_amount_to_merchant_avg_ratio": amount / 75.0,
            "tx_hour_of_day": hour,
            "tx_day_of_week": dow,
            "tx_is_weekend": (dow >= 5),
            "tx_is_night": is_night,
            "tx_seconds_since_last_customer_tx": float(rng.uniform(120.0, 7200.0)),
            "tx_distance_from_home_km": float(rng.uniform(300.0, 3000.0) if is_fraud else rng.uniform(1.0, 30.0)),
            "tx_distance_from_last_tx_km": float(rng.uniform(100.0, 800.0) if is_fraud else rng.uniform(0.5, 15.0)),
            "tx_speed_from_last_tx_kmh": speed,
            "tx_is_impossible_travel": is_impossible_travel,
            "tx_is_foreign_country": is_foreign,
            "tx_is_high_risk_country": is_fraud and bool(rng.choice([True, False], p=[0.3, 0.7])),
            "tx_currency_conversion_required": is_foreign,
            "tx_exchange_rate_markup": 0.025 if is_foreign else 0.0,
            "tx_entry_mode_chip": not is_fraud and bool(rng.choice([True, False], p=[0.8, 0.2])),
            "tx_entry_mode_contactless": bool(rng.choice([True, False], p=[0.3, 0.7])),
            "tx_entry_mode_online": is_fraud or bool(rng.choice([True, False], p=[0.4, 0.6])),
            "tx_entry_mode_fallback_swipe": is_fraud and bool(rng.choice([True, False], p=[0.25, 0.75])),
            "tx_entry_mode_manual_keyed": is_fraud and bool(rng.choice([True, False], p=[0.2, 0.8])),
            "tx_cvv_matched": cvv_matched,
            "tx_avs_street_matched": not is_fraud or bool(rng.choice([True, False], p=[0.5, 0.5])),
            "tx_avs_zip_matched": not is_fraud or bool(rng.choice([True, False], p=[0.5, 0.5])),
            "tx_3ds_authenticated": threeds_auth,
            "tx_3ds_frictionless": threeds_auth and not is_fraud,
            "tx_is_split_bill": split_bill,
            "tx_round_amount_flag": round_amt,
            "tx_just_below_auth_threshold": (980.0 <= amount <= 999.99),
            "tx_auth_latency_ms": int(rng.integers(80, 450)),
            "tx_retry_attempt_count": int(rng.integers(1, 4)) if is_fraud else 0,
            "tx_channel_mobile_app": bool(rng.choice([True, False], p=[0.6, 0.4])),
            "tx_channel_web_browser": bool(rng.choice([True, False], p=[0.3, 0.7])),
            "tx_channel_api": bool(rng.choice([True, False], p=[0.1, 0.9])),
            "is_fraud": 1 if is_fraud else 0,
        })

    events_df = pd.DataFrame(event_rows).sort_values(by="timestamp").reset_index(drop=True)

    feature_tables = {
        "customer": customer_df,
        "merchant": merchant_df,
        "device": device_df,
    }

    return events_df, feature_tables
