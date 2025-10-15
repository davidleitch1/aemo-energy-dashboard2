#!/usr/bin/env python3
"""
Twilio Price Alert System for AEMO Spot Prices
Add this code to your update_spot.py script to send SMS alerts for price thresholds.
"""

from twilio.rest import Client
import os
import pickle
from datetime import datetime
from pathlib import Path

from ..shared.config import config
from ..shared.logging_config import get_logger

# Set up logging
logger = get_logger(__name__)

# Twilio Configuration - now from shared config
TWILIO_ACCOUNT_SID = config.twilio_account_sid
TWILIO_AUTH_TOKEN = config.twilio_auth_token
TWILIO_PHONE_NUMBER = config.twilio_phone_number
MY_PHONE_NUMBER = config.my_phone_number

# Validate that all required variables are set
if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER, MY_PHONE_NUMBER]):
    missing = [var for var, val in [
        ('TWILIO_ACCOUNT_SID', TWILIO_ACCOUNT_SID),
        ('TWILIO_AUTH_TOKEN', TWILIO_AUTH_TOKEN), 
        ('TWILIO_PHONE_NUMBER', TWILIO_PHONE_NUMBER),
        ('MY_PHONE_NUMBER', MY_PHONE_NUMBER)
    ] if not val]
    raise ValueError(f"Missing required environment variables: {missing}")

# Price Thresholds - now from config
HIGH_THRESHOLD = config.high_price_threshold
LOW_THRESHOLD = config.low_price_threshold  
EXTREME_THRESHOLD = config.extreme_price_threshold

# Alert state tracking file - use data directory from config
ALERT_STATE_FILE = Path(config.data_dir) / "price_alert_state.pkl"


def initialize_twilio_client():
    """
    Initialize Twilio client with credentials.
    """
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Twilio client: {e}")
        return None


def load_alert_state():
    """
    Load the current alert state for all regions.
    Returns dict with region: {'high_alert': bool, 'high_time': datetime, 'last_price': float}
    """
    try:
        if os.path.exists(ALERT_STATE_FILE):
            with open(ALERT_STATE_FILE, 'rb') as f:
                return pickle.load(f)
        else:
            # Initialize empty state for all regions
            return {
                'NSW1': {'high_alert': False, 'high_time': None, 'last_price': 0},
                'QLD1': {'high_alert': False, 'high_time': None, 'last_price': 0},
                'SA1': {'high_alert': False, 'high_time': None, 'last_price': 0},
                'TAS1': {'high_alert': False, 'high_time': None, 'last_price': 0},
                'VIC1': {'high_alert': False, 'high_time': None, 'last_price': 0}
            }
    except Exception as e:
        logger.error(f"Error loading alert state: {e}")
        return {}


def save_alert_state(alert_state):
    """
    Save the current alert state to file.
    """
    try:
        os.makedirs(os.path.dirname(ALERT_STATE_FILE), exist_ok=True)
        with open(ALERT_STATE_FILE, 'wb') as f:
            pickle.dump(alert_state, f)
    except Exception as e:
        logger.error(f"Error saving alert state: {e}")


def send_sms(client, message):
    """
    Send SMS using Twilio.
    """
    try:
        message = client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=MY_PHONE_NUMBER
        )
        logger.info(f"SMS sent successfully: {message.sid}")
        return True
    except Exception as e:
        logger.error(f"Failed to send SMS: {e}")
        return False


def check_price_alerts(new_data_df):
    """
    Check new price data for threshold breaches and send alerts.
    
    Args:
        new_data_df: DataFrame with SETTLEMENTDATE as index and columns: REGIONID, RRP
    """
    if new_data_df.empty:
        return
    
    # Initialize Twilio client
    client = initialize_twilio_client()
    if not client:
        logger.error("Cannot send alerts - Twilio client not initialized")
        return
    
    # Load current alert state
    alert_state = load_alert_state()
    
    # Process each region's price data
    for settlement_time, row in new_data_df.iterrows():
        region = row['REGIONID']
        price = row['RRP']
        
        # Initialize region state if not exists
        if region not in alert_state:
            alert_state[region] = {'high_alert': False, 'high_time': None, 'last_price': 0}
        
        current_state = alert_state[region]
        
        # Check for high price threshold breach
        if price >= HIGH_THRESHOLD and not current_state['high_alert']:
            # Price has gone above threshold - send alert
            current_state['high_alert'] = True
            current_state['high_time'] = settlement_time
            current_state['last_price'] = price
            
            # Determine if this is an extreme price
            if price >= EXTREME_THRESHOLD:
                emoji = "🚨🚨🚨"
                urgency = "EXTREME"
            else:
                emoji = "⚠️"
                urgency = "HIGH"
            
            message = f"ITK price alert {emoji} {region} {urgency} PRICE: ${price:.2f}/MWh at {settlement_time.strftime('%H:%M on %d/%m/%Y')}. Threshold: ${HIGH_THRESHOLD}"
            
            if send_sms(client, message):
                logger.info(f"High price alert sent for {region}: ${price:.2f}")
            
        # Check for price returning below low threshold
        elif price <= LOW_THRESHOLD and current_state['high_alert']:
            # Price has dropped back below low threshold - send recovery alert
            high_time = current_state['high_time']
            
            current_state['high_alert'] = False
            current_state['last_price'] = price
            
            # Calculate duration more safely
            duration_str = ""
            if high_time is not None:
                try:
                    duration = settlement_time - high_time
                    # Convert to hours and minutes
                    total_minutes = int(duration.total_seconds() / 60)
                    hours = total_minutes // 60
                    minutes = total_minutes % 60
                    if hours > 0:
                        duration_str = f"Duration: {hours}h {minutes}m"
                    else:
                        duration_str = f"Duration: {minutes}m"
                except Exception as e:
                    logger.error(f"Error calculating duration: {e}")
                    duration_str = "Duration: unknown"
            
            message = f"ITK price alert ✅ {region} PRICE RECOVERED: ${price:.2f}/MWh at {settlement_time.strftime('%H:%M on %d/%m/%Y')}. Below ${LOW_THRESHOLD}. {duration_str}"
            
            if send_sms(client, message):
                logger.info(f"Recovery alert sent for {region}: ${price:.2f}")
        
        # Always update last known price
        current_state['last_price'] = price
    
    # Save updated alert state after processing all regions
    save_alert_state(alert_state)


def get_current_alert_status():
    """
    Get current alert status for all regions.
    Useful for debugging or status checks.
    """
    alert_state = load_alert_state()
    
    print("Current Price Alert Status:")
    print("=" * 40)
    for region, state in alert_state.items():
        status = "HIGH ALERT" if state['high_alert'] else "Normal"
        last_price = state['last_price']
        high_time = state['high_time']
        
        print(f"{region}: {status} | Last Price: ${last_price:.2f}")
        if high_time:
            print(f"       High Alert Since: {high_time}")
    print("=" * 40)


# Example integration code for update_spot.py
def integrate_alerts_example():
    """
    Example of how to integrate this into your update_spot.py script.
    Add this call after successfully parsing new price data.
    """
    
    # In your update_spot_prices() function, after parsing new data:
    
    # if newer_records.empty:
    #     logging.info("No new prices - no records newer than existing data")
    #     return False
    # 
    # # Check for price alerts BEFORE logging the prices
    # check_price_alerts(newer_records)
    # 
    # # Log the new prices found
    # settlement_time = newer_records.index[0]
    # logging.info(f"New prices found for {settlement_time}:")
    # for settlement_date, row in newer_records.iterrows():
    #     logging.info(f"  {row['REGIONID']}: ${row['RRP']:.2f}")
    
    pass


if __name__ == "__main__":
    # Test the alert system
    print("Twilio Price Alert System")
    print("Update the credentials at the top of this file before use.")
    print("\nCurrent alert status:")
    get_current_alert_status()