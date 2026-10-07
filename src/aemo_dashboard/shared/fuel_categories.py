"""
Centralized Fuel Categories and Configuration

This module provides the single source of truth for fuel type categorization
across the AEMO Energy Dashboard. It addresses CRITICAL ISSUE #1: Renewable
Percentage Calculation by ensuring consistent definitions of renewable fuels,
pumped hydro storage exclusions, and thermal fuel categories.

Key Principles:
1. Pumped hydro storage is NOT renewable energy (it's energy storage)
2. Battery storage is NOT generation (it's energy storage)
3. Transmission is NOT generation (it's energy transfer)
4. Renewable % = (Wind + Solar + Rooftop + Hydro + Biomass) /
                 (Total Generation excl. storage & transmission) * 100

Usage:
    from aemo_dashboard.shared.fuel_categories import (
        RENEWABLE_FUELS,
        MAIN_ROOFTOP_REGIONS,
        ROOFTOP_SUBREGIONS,
        PUMPED_HYDRO_DUIDS,
        EXCLUDED_FROM_GENERATION,
        THERMAL_FUELS
    )

History:
    - 2025-10-15: Created to fix renewable percentage calculation bug
    - 2025-10-15: Added MAIN_ROOFTOP_REGIONS to fix rooftop solar double-counting
    - Pumped hydro DUIDs sourced from /Volumes/davidleitch/aemo_production/data/pumped_hydro_duids.txt
    - Based on audit conducted August 3, 2025 (see CLAUDE.md)
"""

# =============================================================================
# RENEWABLE FUELS
# =============================================================================

RENEWABLE_FUELS = [
    'Wind',
    'Solar',
    'Rooftop Solar',
    'Rooftop',  # Alias for Rooftop Solar
    'Water',    # Hydro excluding pumped hydro
    'Hydro',    # Alias for Water
    'Biomass'
]

# =============================================================================
# ROOFTOP SOLAR MAIN REGIONS
# =============================================================================

# These 5 regions represent the main NEM regions for rooftop solar calculations.
# Sub-regions (QLDN, QLDS, QLDC, TASN, TASS) are geographic subsets and must
# NOT be included in NEM totals to avoid double-counting.
#
# Historical Context:
# - QLD1 includes all rooftop solar in Queensland (QLDN + QLDS + QLDC are subsets)
# - TAS1 includes all rooftop solar in Tasmania (TASN + TASS are subsets)
# - Including sub-regions in calculations resulted in ~33% overestimation
#
# Data Collection:
# - The collector continues downloading all regions (including sub-regions)
# - This preserves granular data for future analysis
# - Only calculations filter to these 5 main regions
#
# History:
# - 2025-10-15: Created to fix rooftop solar double-counting issue

MAIN_ROOFTOP_REGIONS = [
    'NSW1',  # New South Wales (complete region)
    'QLD1',  # Queensland (includes QLDN, QLDS, QLDC as subsets)
    'VIC1',  # Victoria (complete region)
    'SA1',   # South Australia (complete region)
    'TAS1'   # Tasmania (includes TASN, TASS as subsets)
]

# Sub-regions to EXCLUDE from calculations (geographic subsets, not additional generation)
ROOFTOP_SUBREGIONS = [
    'QLDN',  # Queensland North (subset of QLD1)
    'QLDS',  # Queensland South (subset of QLD1)
    'QLDC',  # Queensland Central (subset of QLD1)
    'TASN',  # Tasmania North (subset of TAS1)
    'TASS'   # Tasmania South (subset of TAS1)
]

# =============================================================================
# PUMPED HYDRO STORAGE DUIDS
# =============================================================================

# These are the pumped-storage generating units in the NEM: they generate
# from water pumped uphill, so renewable-share calculations that follow
# principle 1 exclude them. Corrected 2026-10-08: the August 2025 list held
# 20 conventional hydro stations (Guthega, Murray, Upper Tumut, Tasmanian
# stations, ...) and missed W/HOE#1. Tumut 3 also runs on natural inflow.
#
# Their pumps are separate load DUIDs (PUMP_LOAD_DUIDS below). Pump loads
# report consumption as positive SCADA MW and carry fuel NULL in
# duid_mapping so the generation views leave them out.

PUMPED_HYDRO_DUIDS = [
    'TUMUT3',     # Tumut 3, NSW, 1,500 MW (Snowy Hydro)
    'SHGEN',      # Shoalhaven (Bendeela + Kangaroo Valley), NSW, 240 MW (Origin)
    'W/HOE#1',    # Wivenhoe 1, QLD, 285 MW (CleanCo)
    'W/HOE#2',    # Wivenhoe 2, QLD, 285 MW (CleanCo)
    'KIDSPHG1',   # Kidston 1, QLD, 125 MW (Genex)
    'KIDSPHG2',   # Kidston 2, QLD, 125 MW (Genex)
]

PUMP_LOAD_DUIDS = [
    'SNOWYP',     # Tumut 3 pumps
    'SHPUMP',     # Shoalhaven pumps
    'PUMP1',      # Wivenhoe pump 1
    'PUMP2',      # Wivenhoe pump 2
    'KIDSPHL1',   # Kidston pump 1
    'KIDSPHL2',   # Kidston pump 2
]

# =============================================================================
# EXCLUDED FROM GENERATION CALCULATIONS
# =============================================================================

# These fuel types/categories should be excluded from BOTH the numerator
# (renewable) and denominator (total generation) when calculating renewable
# percentage, as they represent storage or transmission, not generation.

EXCLUDED_FROM_GENERATION = [
    'Battery Storage',      # Energy storage, not generation
    'Battery Discharging',  # Alias for Battery Storage
    'Battery Charging',     # Negative generation (consumption)
    'Transmission Flow',    # Energy transfer between regions
    'Transmission Exports', # Interstate exports
    'Transmission Imports', # Interstate imports
    'Pumped Hydro'         # If already categorized separately
]

# =============================================================================
# THERMAL FUELS
# =============================================================================

THERMAL_FUELS = [
    'Coal',
    'Black Coal',
    'Brown Coal',
    'Gas',
    'Gas other',
    'CCGT',          # Combined Cycle Gas Turbine
    'OCGT',          # Open Cycle Gas Turbine
    'Gas (CCGT)',    # Alias
    'Gas (OCGT)',    # Alias
    'Gas (Steam)',   # Steam gas turbines
    'Distillate',    # Diesel/oil-fired generation
    'Kerosene'       # Oil-fired generation
]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def is_renewable(fuel_type: str) -> bool:
    """
    Check if a fuel type is renewable.

    Args:
        fuel_type: Fuel type string (e.g., 'Wind', 'Coal', 'Solar')

    Returns:
        bool: True if fuel type is renewable, False otherwise

    Examples:
        >>> is_renewable('Wind')
        True
        >>> is_renewable('Coal')
        False
        >>> is_renewable('Battery Storage')
        False
    """
    return fuel_type in RENEWABLE_FUELS


def is_thermal(fuel_type: str) -> bool:
    """
    Check if a fuel type is thermal (fossil fuel).

    Args:
        fuel_type: Fuel type string

    Returns:
        bool: True if fuel type is thermal, False otherwise
    """
    return fuel_type in THERMAL_FUELS


def is_excluded_from_generation(fuel_type: str) -> bool:
    """
    Check if a fuel type should be excluded from generation calculations.

    This includes storage (batteries, pumped hydro) and transmission flows.

    Args:
        fuel_type: Fuel type string

    Returns:
        bool: True if should be excluded, False otherwise
    """
    return fuel_type in EXCLUDED_FROM_GENERATION


def is_pumped_hydro(duid: str) -> bool:
    """
    Check if a DUID is a pumped hydro storage facility.

    Args:
        duid: DUID (Dispatchable Unit Identifier) string

    Returns:
        bool: True if DUID is pumped hydro, False otherwise

    Examples:
        >>> is_pumped_hydro('TUMUT3')
        True
        >>> is_pumped_hydro('LIDDELL1')
        False
    """
    return duid in PUMPED_HYDRO_DUIDS


def get_fuel_category(fuel_type: str) -> str:
    """
    Get the broad category for a fuel type.

    Args:
        fuel_type: Fuel type string

    Returns:
        str: 'renewable', 'thermal', 'storage', or 'other'

    Examples:
        >>> get_fuel_category('Wind')
        'renewable'
        >>> get_fuel_category('Coal')
        'thermal'
        >>> get_fuel_category('Battery Storage')
        'storage'
    """
    if is_renewable(fuel_type):
        return 'renewable'
    elif is_thermal(fuel_type):
        return 'thermal'
    elif is_excluded_from_generation(fuel_type):
        return 'storage'
    else:
        return 'other'


# =============================================================================
# VALIDATION
# =============================================================================

def validate_configuration():
    """
    Validate that the configuration is internally consistent.

    Raises:
        AssertionError: If configuration has inconsistencies
    """
    # Check no overlaps between renewable and thermal
    renewable_set = set(RENEWABLE_FUELS)
    thermal_set = set(THERMAL_FUELS)
    overlap = renewable_set & thermal_set
    assert not overlap, f"Renewable and thermal fuels overlap: {overlap}"

    # Check PUMPED_HYDRO_DUIDS has correct count
    assert len(PUMPED_HYDRO_DUIDS) == 6, \
        f"Expected 6 pumped hydro DUIDs, got {len(PUMPED_HYDRO_DUIDS)}"
    assert not set(PUMPED_HYDRO_DUIDS) & set(PUMP_LOAD_DUIDS), \
        "A DUID is both a pumped hydro generator and a pump load"

    # Check no duplicate DUIDs
    assert len(PUMPED_HYDRO_DUIDS) == len(set(PUMPED_HYDRO_DUIDS)), \
        "Duplicate DUIDs in PUMPED_HYDRO_DUIDS"

    return True


# Run validation on import
validate_configuration()


if __name__ == '__main__':
    # Self-test
    print("Fuel Categories Configuration")
    print("=" * 80)
    print(f"\nRenewable Fuels ({len(RENEWABLE_FUELS)}):")
    for fuel in RENEWABLE_FUELS:
        print(f"  - {fuel}")

    print(f"\nThermal Fuels ({len(THERMAL_FUELS)}):")
    for fuel in THERMAL_FUELS:
        print(f"  - {fuel}")

    print(f"\nExcluded from Generation ({len(EXCLUDED_FROM_GENERATION)}):")
    for fuel in EXCLUDED_FROM_GENERATION:
        print(f"  - {fuel}")

    print(f"\nPumped Hydro DUIDs ({len(PUMPED_HYDRO_DUIDS)}):")
    for duid in PUMPED_HYDRO_DUIDS:
        print(f"  - {duid}")

    print("\n" + "=" * 80)
    print("Configuration validation: PASSED")

    # Test functions
    print("\nFunction Tests:")
    print(f"  is_renewable('Wind'): {is_renewable('Wind')}")
    print(f"  is_renewable('Coal'): {is_renewable('Coal')}")
    print(f"  is_thermal('Coal'): {is_thermal('Coal')}")
    print(f"  is_pumped_hydro('TUMUT3'): {is_pumped_hydro('TUMUT3')}")
    print(f"  is_pumped_hydro('LIDDELL1'): {is_pumped_hydro('LIDDELL1')}")
    print(f"  get_fuel_category('Wind'): {get_fuel_category('Wind')}")
    print(f"  get_fuel_category('Battery Storage'): {get_fuel_category('Battery Storage')}")
