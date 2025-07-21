#!/usr/bin/env python3
"""
Utility script to manage DUID exceptions for the Energy Dashboard
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Path to the exception file
BASE_PATH = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot")
EXCEPTION_FILE = BASE_PATH / "duid_exceptions.json"

def load_exceptions():
    """Load the current exception list"""
    if EXCEPTION_FILE.exists():
        with open(EXCEPTION_FILE, 'r') as f:
            data = json.load(f)
            return set(data.get('exception_duids', []))
    return set()

def save_exceptions(exception_duids):
    """Save the exception list"""
    data = {
        'exception_duids': sorted(list(exception_duids)),
        'last_updated': datetime.now().isoformat(),
        'note': 'DUIDs in this list will not trigger email alerts'
    }
    with open(EXCEPTION_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(exception_duids)} DUIDs to exception list")

def list_exceptions():
    """List all DUIDs in the exception list"""
    exceptions = load_exceptions()
    if not exceptions:
        print("No DUIDs in exception list")
        return
    
    print(f"DUIDs in exception list ({len(exceptions)} total):")
    for duid in sorted(exceptions):
        print(f"  - {duid}")

def add_duid(duid):
    """Add a DUID to the exception list"""
    exceptions = load_exceptions()
    if duid in exceptions:
        print(f"{duid} is already in the exception list")
    else:
        exceptions.add(duid)
        save_exceptions(exceptions)
        print(f"Added {duid} to exception list")

def remove_duid(duid):
    """Remove a DUID from the exception list"""
    exceptions = load_exceptions()
    if duid not in exceptions:
        print(f"{duid} is not in the exception list")
    else:
        exceptions.remove(duid)
        save_exceptions(exceptions)
        print(f"Removed {duid} from exception list")

def clear_all():
    """Clear all DUIDs from the exception list"""
    response = input("Are you sure you want to clear all DUIDs from the exception list? (yes/no): ")
    if response.lower() == 'yes':
        save_exceptions(set())
        print("Cleared all DUIDs from exception list")
    else:
        print("Operation cancelled")

def main():
    """Main command line interface"""
    if len(sys.argv) < 2:
        print("DUID Exception List Manager")
        print("Usage:")
        print("  python manage_duid_exceptions.py list              - List all DUIDs in exception list")
        print("  python manage_duid_exceptions.py add <DUID>        - Add a DUID to exception list")
        print("  python manage_duid_exceptions.py remove <DUID>     - Remove a DUID from exception list")
        print("  python manage_duid_exceptions.py clear             - Clear all DUIDs from exception list")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'list':
        list_exceptions()
    elif command == 'add' and len(sys.argv) >= 3:
        add_duid(sys.argv[2])
    elif command == 'remove' and len(sys.argv) >= 3:
        remove_duid(sys.argv[2])
    elif command == 'clear':
        clear_all()
    else:
        print(f"Unknown command: {command}")
        print("Run without arguments to see usage")

if __name__ == "__main__":
    main()