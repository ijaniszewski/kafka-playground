"""
Generator for transaction events for mobile game "Crystal Quest"
Simulates in-app purchases with refunds and realistic pricing
"""

import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


class PurchaseEventGenerator:
    """Generates realistic in-app purchase events"""
    
    # IAP items catalog
    ITEMS = {
        "virtual_currency": [
            {"id": "com.crystal_quest.coin_pack_small", "name": "Small Coin Pack", "price": 0.99, "quantity": 100},
            {"id": "com.crystal_quest.coin_pack_medium", "name": "Medium Coin Pack", "price": 4.99, "quantity": 550},
            {"id": "com.crystal_quest.coin_pack_large", "name": "Large Coin Pack", "price": 9.99, "quantity": 1200},
            {"id": "com.crystal_quest.coin_pack_mega", "name": "Mega Coin Pack", "price": 19.99, "quantity": 2500},
        ],
        "subscription": [
            {"id": "com.crystal_quest.premium_monthly", "name": "Premium Monthly", "price": 9.99, "quantity": 1},
            {"id": "com.crystal_quest.premium_yearly", "name": "Premium Yearly", "price": 99.99, "quantity": 1},
        ],
        "bundle": [
            {"id": "com.crystal_quest.starter_pack", "name": "Starter Pack", "price": 4.99, "quantity": 1},
            {"id": "com.crystal_quest.ultimate_bundle", "name": "Ultimate Bundle", "price": 29.99, "quantity": 1},
        ],
        "power_up": [
            {"id": "com.crystal_quest.hint_3pack", "name": "Hint 3-Pack", "price": 1.99, "quantity": 3},
            {"id": "com.crystal_quest.timer_boost_5", "name": "Timer Boost x5", "price": 2.99, "quantity": 5},
        ]
    }
    
    PLATFORMS = ["apple_app_store", "google_play"]
    CURRENCIES = {
        "PL": "PLN",
        "US": "USD",
        "DE": "EUR",
        "GB": "GBP",
        "FR": "EUR",
        "JP": "JPY",
        "BR": "BRL",
        "IN": "INR"
    }
    
    # Conversion rates to USD (simplified, normally from external API)
    EXCHANGE_RATES = {
        "USD": 1.0,
        "EUR": 0.92,
        "GBP": 0.79,
        "PLN": 3.95,
        "JPY": 149.50,
        "BRL": 4.95,
        "INR": 83.20
    }
    
    def __init__(self, user_id: str, country: str, game_id: str = "crystal_quest"):
        self.user_id = user_id
        self.game_id = game_id
        self.country = country
        self.currency = self.CURRENCIES.get(country, "USD")
        self.platform = random.choice(self.PLATFORMS)
        
    def generate_purchase(
        self,
        transaction_time: datetime,
        session_id: Optional[str] = None,
        is_sandbox: bool = False
    ) -> Dict[str, Any]:
        """Generate a single purchase event"""
        
        # Select random item type and item
        item_type = random.choice(list(self.ITEMS.keys()))
        item = random.choice(self.ITEMS[item_type])
        
        # Apply promotions (30% chance of discount)
        has_discount = random.random() < 0.3
        original_price = item["price"] if has_discount else None
        final_price = item["price"] * 0.75 if has_discount else item["price"]
        
        # Convert to local currency
        exchange_rate = self.EXCHANGE_RATES.get(self.currency, 1.0)
        local_price = final_price * exchange_rate
        
        purchase_id = f"tx_{uuid.uuid4().hex[:8]}"
        
        event = {
            "purchase_id": purchase_id,
            "user_id": self.user_id,
            "game_id": self.game_id,
            "transaction_ts": transaction_time.isoformat() + "Z",
            "item_id": item["id"],
            "item_name": item["name"],
            "item_type": item_type,
            "quantity": item["quantity"],
            "price": round(local_price, 2),
            "price_usd": round(final_price, 2),
            "original_price": round(original_price * exchange_rate, 2) if original_price else None,
            "currency": self.currency,
            "platform": self.platform,
            "is_refund": False,
            "sandbox": is_sandbox
        }
        
        # Add session_id if purchase happened during session
        if session_id:
            event["session_id"] = session_id
        
        return event
    
    def generate_refund_event(self, original_purchase: Dict[str, Any], refund_time: datetime) -> Dict[str, Any]:
        """Generate a refund event (same purchase_id, is_refund=True)"""
        refund_event = original_purchase.copy()
        refund_event["transaction_ts"] = refund_time.isoformat() + "Z"
        refund_event["is_refund"] = True
        refund_event["refund_reason"] = random.choice([
            "customer_request",
            "fraudulent",
            "accidental_purchase",
            "technical_issue"
        ])
        return refund_event


def generate_purchases_for_sessions(
    user_sessions: List[Dict[str, Any]],
    purchase_probability: float = 0.15,
    refund_rate: float = 0.03
) -> List[Dict[str, Any]]:
    """
    Generate purchases that align with gameplay sessions
    
    Args:
        user_sessions: List of session_start events with user info
        purchase_probability: Chance of purchase during a session
        refund_rate: Percentage of purchases that get refunded
    """
    purchases = []
    refund_candidates = []
    
    # Group events by session
    sessions = {}
    for event in user_sessions:
        if event["event_type"] == "session_start":
            session_id = event["session_id"]
            sessions[session_id] = {
                "user_id": event["user_id"],
                "session_id": session_id,
                "start_ts": datetime.fromisoformat(event["event_ts"].replace("Z", "")),
                "country": event["metadata"]["country"]
            }
    
    # Generate purchases for some sessions
    for session_id, session_info in sessions.items():
        if random.random() < purchase_probability:
            # Purchase happens sometime during the session
            purchase_time = session_info["start_ts"] + timedelta(
                seconds=random.randint(30, 300)  # 30s to 5min into session
            )
            
            generator = PurchaseEventGenerator(
                user_id=session_info["user_id"],
                country=session_info["country"]
            )
            
            purchase = generator.generate_purchase(
                transaction_time=purchase_time,
                session_id=session_id
            )
            purchases.append(purchase)
            
            # Mark for potential refund
            if random.random() < refund_rate:
                refund_candidates.append({
                    "purchase": purchase,
                    "generator": generator
                })
    
    # Generate refund events (5 minutes to 24 hours after purchase)
    for candidate in refund_candidates:
        original_purchase = candidate["purchase"]
        generator = candidate["generator"]
        
        purchase_time = datetime.fromisoformat(
            original_purchase["transaction_ts"].replace("Z", "")
        )
        refund_time = purchase_time + timedelta(
            minutes=random.randint(5, 1440)  # 5min to 24h
        )
        
        refund_event = generator.generate_refund_event(original_purchase, refund_time)
        purchases.append(refund_event)
    
    # Sort by timestamp
    purchases.sort(key=lambda x: x["transaction_ts"])
    return purchases


def simulate_device_change(user_id: str, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Simulate a user changing device mid-stream
    This tests SCD Type 2 in dim_user table
    """
    user_events = [e for e in events if e.get("user_id") == user_id]
    if len(user_events) < 2:
        return events
    
    # Find midpoint
    midpoint = len(user_events) // 2
    change_time = datetime.fromisoformat(user_events[midpoint]["event_ts"].replace("Z", ""))
    
    # Change device in all events after midpoint
    for event in events:
        if event.get("user_id") == user_id:
            event_time = datetime.fromisoformat(event["event_ts"].replace("Z", ""))
            if event_time >= change_time and "metadata" in event:
                # Switch from Android to iOS or vice versa
                if event["metadata"]["os"] == "ios":
                    event["metadata"]["os"] = "android"
                    event["metadata"]["os_ver"] = "14"
                    event["metadata"]["device_model"] = "Pixel 8 Pro"
                else:
                    event["metadata"]["os"] = "ios"
                    event["metadata"]["os_ver"] = "17.4"
                    event["metadata"]["device_model"] = "iPhone15,2"
    
    return events


if __name__ == "__main__":
    # Test generation
    from gameplay_generator import generate_user_sessions
    
    # Generate gameplay sessions first
    gameplay_events = generate_user_sessions(num_users=5, sessions_per_user=2)
    
    # Generate purchases aligned with sessions
    purchase_events = generate_purchases_for_sessions(
        gameplay_events,
        purchase_probability=0.3,  # Higher for testing
        refund_rate=0.1  # Higher for testing
    )
    
    print(f"Generated {len(purchase_events)} purchase events")
    print(f"Refunds: {sum(1 for p in purchase_events if p['is_refund'])}")
    
    print("\nFirst 2 purchase events:")
    import json
    for event in purchase_events[:2]:
        print(json.dumps(event, indent=2))
