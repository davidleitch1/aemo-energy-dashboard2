"""
Station Search Engine - Fuzzy search functionality for stations and DUIDs.

This module provides approximate search capabilities to help users find stations
by name or DUID with auto-suggestions and fuzzy matching.
"""

import pandas as pd
from typing import Dict, List, Tuple, Optional
from fuzzywuzzy import fuzz, process
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

class StationSearchEngine:
    """Fuzzy search engine for stations and DUIDs with aggregation support"""
    
    def __init__(self, duid_mapping):
        """
        Initialize the search engine with DUID mapping data.
        
        Args:
            duid_mapping: DataFrame or Dictionary mapping DUIDs to station information
        """
        self.duid_mapping = duid_mapping
        self.search_index = self._build_search_index()
        self.station_index = self._build_station_index()  # New: Station-level aggregation
        logger.info(f"Search engine initialized with {len(self.search_index)} DUIDs and {len(self.station_index)} stations")
    
    def _build_search_index(self) -> List[Dict]:
        """
        Build a searchable index from the DUID mapping.
        
        Returns:
            List of dictionaries containing searchable station information
        """
        index = []
        
        # Handle DataFrame format
        if hasattr(self.duid_mapping, 'iterrows'):
            # It's a DataFrame - transpose to get DUIDs as index
            if 'DUID' in self.duid_mapping.columns:
                # DUIDs are in a column
                for _, row in self.duid_mapping.iterrows():
                    duid = row['DUID']
                    station_name = row.get('Site Name', row.get('Station Name', '')).strip() if pd.notna(row.get('Site Name', row.get('Station Name', ''))) else ''
                    owner = row.get('Owner', '').strip() if pd.notna(row.get('Owner', '')) else ''
                    region = row.get('Region', '').strip() if pd.notna(row.get('Region', '')) else ''
                    capacity = row.get('Capacity(MW)', row.get('Nameplate Capacity (MW)', 0))
                    capacity = capacity if pd.notna(capacity) else 0
                    
                    search_entry = {
                        'duid': duid,
                        'station_name': station_name,
                        'owner': owner,
                        'fuel': '',  # May not be available in this format
                        'region': region,
                        'capacity_mw': float(capacity) if capacity else 0,
                        'searchable_text': f"{duid} {station_name} {owner}".lower(),
                        'display_name': f"{station_name} ({duid})" if station_name else duid,
                        'full_info': row.to_dict()
                    }
                    index.append(search_entry)
            else:
                # DUIDs are the index, transpose the DataFrame
                df_T = self.duid_mapping.T
                for duid in df_T.columns:
                    try:
                        station_name = df_T.loc['Site Name', duid] if 'Site Name' in df_T.index else ''
                        owner = df_T.loc['Owner', duid] if 'Owner' in df_T.index else ''
                        region = df_T.loc['Region', duid] if 'Region' in df_T.index else ''
                        capacity = df_T.loc['Capacity(MW)', duid] if 'Capacity(MW)' in df_T.index else 0
                        
                        search_entry = {
                            'duid': duid,
                            'station_name': str(station_name).strip() if pd.notna(station_name) else '',
                            'owner': str(owner).strip() if pd.notna(owner) else '',
                            'fuel': '',
                            'region': str(region).strip() if pd.notna(region) else '',
                            'capacity_mw': float(capacity) if pd.notna(capacity) and capacity else 0,
                            'searchable_text': f"{duid} {station_name} {owner}".lower(),
                            'display_name': f"{station_name} ({duid})" if station_name else duid,
                            'full_info': df_T[duid].to_dict()
                        }
                        index.append(search_entry)
                    except Exception as e:
                        logger.warning(f"Error processing DUID {duid}: {e}")
                        continue
        else:
            # Handle dictionary format (fallback)
            for duid, info in self.duid_mapping.items():
                station_name = info.get('Station Name', '').strip()
                owner = info.get('Owner', '').strip()
                fuel = info.get('Fuel', '').strip()
                region = info.get('Region', '').strip()
                
                search_entry = {
                    'duid': duid,
                    'station_name': station_name,
                    'owner': owner,
                    'fuel': fuel,
                    'region': region,
                    'capacity_mw': info.get('Nameplate Capacity (MW)', 0),
                    'searchable_text': f"{duid} {station_name} {owner} {fuel}".lower(),
                    'display_name': f"{station_name} ({duid})" if station_name else duid,
                    'full_info': info
                }
                index.append(search_entry)
        
        return index
    
    def _build_station_index(self) -> List[Dict]:
        """
        Build station-level aggregated index grouping DUIDs by base name pattern.
        
        Groups DUIDs that share the same base name (e.g., ER01, ER02, ER03, ER04 -> "ER")
        This is more reliable than using Site Name as it follows AEMO naming conventions.
        
        Returns:
            List of dictionaries containing aggregated station information
        """
        import re
        stations = {}
        
        # Group all DUIDs by their base name pattern
        for entry in self.search_index:
            duid = entry['duid']
            
            # Extract base station identifier (remove trailing digits)
            match = re.match(r'^(.+?)(\d+)$', duid)
            if match:
                base_name = match.group(1)  # e.g., "ER" from "ER01"
            else:
                base_name = duid  # Single unit or non-standard naming
                
            if base_name not in stations:
                stations[base_name] = {
                    'station_base': base_name,
                    'station_name': entry['station_name'] or base_name,  # Use site name if available
                    'duids': [],
                    'total_capacity_mw': 0,
                    'fuel_types': set(),
                    'region': entry['region'],
                    'owner': entry['owner'],
                    'unit_count': 0
                }
            
            # Aggregate information
            stations[base_name]['duids'].append(entry['duid'])
            stations[base_name]['total_capacity_mw'] += entry['capacity_mw'] or 0
            stations[base_name]['fuel_types'].add(entry['fuel'])
            stations[base_name]['unit_count'] += 1
        
        # Convert to list format for consistency with search_index
        station_list = []
        for base_name, info in stations.items():
            # Convert fuel_types set to sorted string
            fuel_str = ', '.join(sorted([f for f in info['fuel_types'] if f]))
            
            # Only include multi-unit stations (more than 1 DUID)
            if info['unit_count'] > 1:
                station_entry = {
                    'station_name': info['station_name'],
                    'station_base': base_name,
                    'duids': info['duids'],  # List of all DUIDs for this station
                    'total_capacity_mw': info['total_capacity_mw'],
                    'fuel': fuel_str,
                    'region': info['region'],
                    'owner': info['owner'],
                    'unit_count': info['unit_count'],
                    'searchable_text': f"{info['station_name']} {base_name} {info['owner']} {fuel_str}".lower(),
                    'display_name': f"{info['station_name']} ({info['unit_count']} units, {info['total_capacity_mw']:.0f} MW)",
                    'is_station': True  # Flag to distinguish from individual DUIDs
                }
                station_list.append(station_entry)
        
        # Sort by total capacity (largest first)
        station_list.sort(key=lambda x: x['total_capacity_mw'], reverse=True)
        
        logger.info(f"Built station index with {len(station_list)} aggregated stations")
        return station_list
    
    def fuzzy_search(self, query: str, limit: int = 10, min_score: int = 60, mode: str = 'duid') -> List[Dict]:
        """
        Perform fuzzy search on stations and DUIDs.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            min_score: Minimum fuzzy match score (0-100)
            mode: Search mode - 'duid' for individual units, 'station' for aggregated stations
            
        Returns:
            List of matching station dictionaries with scores
        """
        if not query or len(query.strip()) < 2:
            return []
        
        query = query.lower().strip()
        results = []
        
        # Choose which index to search based on mode
        search_data = self.station_index if mode == 'station' else self.search_index
        
        for entry in search_data:
            # Calculate fuzzy match scores for different fields
            if mode == 'station':
                # For station mode, no DUID matching - focus on station name
                name_score = fuzz.partial_ratio(query, entry['station_name'].lower())
                text_score = fuzz.partial_ratio(query, entry['searchable_text'])
                best_score = max(name_score, text_score)
                
                # Boost exact station name matches
                if query.lower() in entry['station_name'].lower():
                    best_score = min(100, best_score + 20)
            else:
                # For DUID mode, include DUID scoring
                duid_score = fuzz.partial_ratio(query, entry['duid'].lower())
                name_score = fuzz.partial_ratio(query, entry['station_name'].lower())
                text_score = fuzz.partial_ratio(query, entry['searchable_text'])
                best_score = max(duid_score, name_score, text_score)
                
                # Boost exact DUID matches
                if query.upper() == entry['duid'].upper():
                    best_score = 100
            
            if best_score >= min_score:
                result = entry.copy()
                result['score'] = best_score
                results.append(result)
        
        # Sort by score (descending) and limit results
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:limit]
    
    def get_suggestions(self, partial_query: str, limit: int = 5) -> List[str]:
        """
        Get auto-complete suggestions for partial queries.
        
        Args:
            partial_query: Partial search string
            limit: Maximum number of suggestions
            
        Returns:
            List of suggestion strings
        """
        if not partial_query or len(partial_query.strip()) < 1:
            return []
        
        suggestions = []
        query = partial_query.lower().strip()
        
        for entry in self.search_index:
            # Check if any field starts with the query
            if (entry['duid'].lower().startswith(query) or 
                entry['station_name'].lower().startswith(query)):
                suggestions.append(entry['display_name'])
        
        # Remove duplicates and limit
        suggestions = list(dict.fromkeys(suggestions))  # Preserves order while removing duplicates
        return suggestions[:limit]
    
    def get_exact_match(self, query: str) -> Optional[Dict]:
        """
        Get exact match for a DUID or station name.
        
        Args:
            query: Exact search query
            
        Returns:
            Station dictionary if found, None otherwise
        """
        query = query.strip()
        
        for entry in self.search_index:
            if (entry['duid'].upper() == query.upper() or 
                entry['station_name'].lower() == query.lower()):
                return entry
        
        return None
    
    def get_stations_by_fuel(self, fuel_type: str) -> List[Dict]:
        """
        Get all stations of a specific fuel type.
        
        Args:
            fuel_type: Fuel type to filter by
            
        Returns:
            List of station dictionaries
        """
        return [entry for entry in self.search_index 
                if entry['fuel'].lower() == fuel_type.lower()]
    
    def get_stations_by_region(self, region: str) -> List[Dict]:
        """
        Get all stations in a specific region.
        
        Args:
            region: Region to filter by
            
        Returns:
            List of station dictionaries
        """
        return [entry for entry in self.search_index 
                if entry['region'].upper() == region.upper()]
    
    def get_popular_stations(self, limit: int = 10, mode: str = 'duid') -> List[Dict]:
        """
        Get popular/large stations (by capacity).
        
        Args:
            limit: Maximum number of stations to return
            mode: 'duid' for individual units, 'station' for aggregated stations
            
        Returns:
            List of station dictionaries sorted by capacity
        """
        if mode == 'station':
            # Return top stations by total capacity
            return self.station_index[:limit]  # Already sorted by capacity in _build_station_index
        else:
            # Sort by individual DUID capacity (descending) and return top stations
            sorted_stations = sorted(self.search_index, 
                                   key=lambda x: x['capacity_mw'] or 0, 
                                   reverse=True)
            return sorted_stations[:limit]
    
    def search_stats(self) -> Dict:
        """
        Get search index statistics.
        
        Returns:
            Dictionary with search statistics
        """
        total_stations = len(self.search_index)
        fuel_types = set(entry['fuel'] for entry in self.search_index if entry['fuel'])
        regions = set(entry['region'] for entry in self.search_index if entry['region'])
        
        return {
            'total_stations': total_stations,
            'fuel_types': sorted(fuel_types),
            'regions': sorted(regions),
            'total_capacity_mw': sum(entry['capacity_mw'] or 0 for entry in self.search_index)
        }
    
    def get_station_info(self, duid: str) -> Dict:
        """
        Get station information for a specific DUID.
        
        Args:
            duid: The DUID to look up
            
        Returns:
            Dictionary with station information
        """
        # Find the station in our search index (which handles the DataFrame format properly)
        for entry in self.search_index:
            if entry['duid'] == duid:
                return entry
        
        return {'duid': duid, 'station_name': duid, 'display_name': duid}