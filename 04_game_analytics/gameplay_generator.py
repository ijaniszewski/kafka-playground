"""
Generator for player activity events for mobile game "Crystal Quest"  
Simulates realistic session flows with proper event sequencing
"""

import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any


class GameplayEventGenerator:
    """Generates realistic player activity event sequences for a mobile game session"""
    
    COUNTRIES = ["PL", "US", "DE", "GB", "FR", "JP", "BR", "IN"]
    OS_TYPES = ["ios", "android"]
    IOS_DEVICES = [
        {"model": "iPhone15,2", "os_ver": "17.4"},
        {"model": "iPhone14,3", "os_ver": "17.2"},
        {"model": "iPhone13,4", "os_ver": "16.5"},
    ]
    ANDROID_DEVICES = [
        {"model": "SM-G998B", "os_ver": "14"},  # Samsung S21 Ultra
        {"model": "Pixel 8 Pro", "os_ver": "14"},
        {"model": "OnePlus 11", "os_ver": "13"},
    ]
    NETWORK_TYPES = ["wifi", "4g", "5g"]
    DIFFICULTIES = ["easy", "medium", "hard", "extreme"]
    AD_NETWORKS = ["IronSource", "AdMob", "Unity Ads", "AppLovin"]
    AD_PLACEMENTS = ["revive_life", "level_complete_bonus", "main_menu", "between_levels"]
    REWARD_TYPES = ["soft_currency", "extra_life", "power_up", "time_skip"]
    
    def __init__(self, user_id: str, game_id: str = "crystal_quest"):
        self.user_id = user_id
        self.game_id = game_id
        self.session_id = f"play_{uuid.uuid4().hex[:12]}"
        self.app_version = f"2.{random.randint(10, 15)}.{random.randint(0, 5)}"
        
        # User profile (persistent across sessions)
        self.os = random.choice(self.OS_TYPES)
        if self.os == "ios":
            device = random.choice(self.IOS_DEVICES)
        else:
            device = random.choice(self.ANDROID_DEVICES)
        
        self.device_model = device["model"]
        self.os_ver = device["os_ver"]
        self.country = random.choice(self.COUNTRIES)
        self.network_type = random.choice(self.NETWORK_TYPES)
        
    def generate_session(self, start_time: datetime) -> List[Dict[str, Any]]:
        """Generate a complete play session with realistic event sequence"""
        events = []
        current_time = start_time
        
        # 1. Session Start
        events.append(self._create_session_start(current_time))
        current_time += timedelta(seconds=random.randint(1, 3))
        
        # 2. Puzzle Events (1-5 puzzles per session)
        num_puzzles = random.randint(1, 5)
        for puzzle_num in range(1, num_puzzles + 1):
            puzzle_events, puzzle_end_time = self._create_puzzle_sequence(current_time, puzzle_num)
            events.extend(puzzle_events)
            
            # Time between puzzles
            current_time = puzzle_end_time + timedelta(seconds=random.randint(2, 10))
            
            # Sometimes watch ad between puzzles (30% chance)
            if random.random() < 0.3:
                events.append(self._create_reward_claim(current_time))
                current_time += timedelta(seconds=random.randint(15, 30))
        
        # Store session end time for purchase alignment
        self.session_start = start_time
        self.session_end = current_time
        
        return events
    
    def _create_session_start(self, timestamp: datetime) -> Dict[str, Any]:
        """Create session_start event"""
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "game_id": self.game_id,
            "event_type": "session_start",
            "event_ts": timestamp.isoformat() + "Z",
            "metadata": {
                "app_version": self.app_version,
                "os": self.os,
                "os_ver": self.os_ver,
                "device_model": self.device_model,
                "network_type": self.network_type,
                "country": self.country
            }
        }
    
    def _create_puzzle_sequence(self, start_time: datetime, puzzle_id: int) -> tuple[List[Dict[str, Any]], datetime]:
        """Create puzzle_start and puzzle_complete events. Returns (events, end_time)"""
        events = []
        
        # Puzzle start
        difficulty = random.choice(self.DIFFICULTIES)
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "game_id": self.game_id,
            "event_type": "puzzle_start",
            "event_ts": start_time.isoformat() + "Z",
            "metadata": {
                "app_version": self.app_version,
                "os": self.os,
                "os_ver": self.os_ver,
                "device_model": self.device_model,
                "network_type": self.network_type,
                "country": self.country,
                "puzzle_id": puzzle_id,
                "difficulty": difficulty
            }
        })
        
        # Puzzle complete (80% success rate)
        duration_ms = random.randint(30000, 180000)  # 30s - 3min
        complete_time = start_time + timedelta(milliseconds=duration_ms)
        result = "solved" if random.random() < 0.8 else "failed"
        
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "game_id": self.game_id,
            "event_type": "puzzle_complete",
            "event_ts": complete_time.isoformat() + "Z",
            "metadata": {
                "app_version": self.app_version,
                "os": self.os,
                "os_ver": self.os_ver,
                "device_model": self.device_model,
                "network_type": self.network_type,
                "country": self.country,
                "puzzle_id": puzzle_id,
                "difficulty": difficulty,
                "result": result,
                "duration_ms": duration_ms,
                "score": random.randint(100, 10000) if result == "solved" else 0
            }
        })
        
        return events, complete_time
    
    def _create_reward_claim(self, timestamp: datetime) -> Dict[str, Any]:
        """Create reward_claim event"""
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "game_id": self.game_id,
            "event_type": "reward_claim",
            "event_ts": timestamp.isoformat() + "Z",
            "metadata": {
                "app_version": self.app_version,
                "os": self.os,
                "os_ver": self.os_ver,
                "device_model": self.device_model,
                "network_type": self.network_type,
                "country": self.country,
                "ad_network": random.choice(self.AD_NETWORKS),
                "placement": random.choice(self.AD_PLACEMENTS),
                "reward_type": random.choice(self.REWARD_TYPES),
                "reward_amount": random.randint(50, 500)
            }
        }


def generate_user_sessions(num_users: int = 10, sessions_per_user: int = 3) -> List[Dict[str, Any]]:
    """Generate multiple sessions for multiple users"""
    all_events = []
    base_time = datetime.utcnow() - timedelta(hours=24)
    
    for user_num in range(1, num_users + 1):
        user_id = f"u_{100000 + user_num}"
        
        for session_num in range(sessions_per_user):
            # Sessions spread across 24 hours
            session_start = base_time + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            generator = GameplayEventGenerator(user_id)
            session_events = generator.generate_session(session_start)
            all_events.extend(session_events)
    
    # Sort by timestamp
    all_events.sort(key=lambda x: x["event_ts"])
    return all_events


if __name__ == "__main__":
    # Test generation
    events = generate_user_sessions(num_users=5, sessions_per_user=2)
    
    print(f"Generated {len(events)} events")
    print("\nFirst 3 events:")
    import json
    for event in events[:3]:
        print(json.dumps(event, indent=2))
