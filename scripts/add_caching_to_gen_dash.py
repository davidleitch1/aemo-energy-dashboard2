#!/usr/bin/env python3
"""
Script to add Panel caching to gen_dash.py
This modifies the existing file to add caching to expensive plot operations
"""

import os
import shutil
from datetime import datetime

def add_caching_to_gen_dash():
    """Add caching decorators and helper functions to gen_dash.py"""
    
    # Paths
    original_file = "src/aemo_dashboard/generation/gen_dash.py"
    backup_file = f"src/aemo_dashboard/generation/gen_dash_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
    
    # Create backup
    print(f"Creating backup: {backup_file}")
    shutil.copy2(original_file, backup_file)
    
    # Read the original file
    with open(original_file, 'r') as f:
        lines = f.readlines()
    
    # Find where to insert cache helpers (after imports)
    import_end_idx = 0
    for i, line in enumerate(lines):
        if line.strip() and not line.startswith('import') and not line.startswith('from') and i > 30:
            import_end_idx = i
            break
    
    # Cache helper code to insert
    cache_helpers = '''
# =============================================================================
# Cache Configuration and Helpers
# =============================================================================

# Enable caching via environment variable
ENABLE_PN_CACHE = os.getenv('ENABLE_PN_CACHE', 'true').lower() == 'true'
logger.info(f"Panel caching: {'enabled' if ENABLE_PN_CACHE else 'disabled'}")

def conditional_cache(**cache_kwargs):
    """Conditional cache decorator that can be disabled"""
    def decorator(func):
        if ENABLE_PN_CACHE:
            return pn.cache(**cache_kwargs)(func)
        return func
    return decorator

def create_plot_cache_key(data_shape: tuple, region: str, time_range: str, 
                         fuel_types: tuple, has_negative: bool) -> str:
    """Create a cache key for plot generation"""
    import hashlib
    key_parts = [
        str(data_shape),
        region,
        time_range,
        str(sorted(fuel_types)),
        str(has_negative)
    ]
    key_str = '|'.join(key_parts)
    return hashlib.md5(key_str.encode()).hexdigest()

# Global cache for plot creation functions
_plot_cache_stats = {'hits': 0, 'misses': 0}

'''
    
    # Insert cache helpers after imports
    lines.insert(import_end_idx, cache_helpers)
    
    # Find create_plot method
    create_plot_idx = None
    for i, line in enumerate(lines):
        if 'def create_plot(self):' in line:
            create_plot_idx = i
            break
    
    if create_plot_idx is None:
        raise ValueError("Could not find create_plot method")
    
    # Add cached plot creation function before create_plot
    cached_plot_function = '''
    @conditional_cache(max_items=20, policy='LRU', ttl=300)  # 5 min TTL
    def _create_generation_plot_cached(self, cache_key: str, plot_data_json: str, 
                                     fuel_types: tuple, fuel_colors_json: str,
                                     has_negative_values: bool, region: str, 
                                     time_range_display: str):
        """Cached plot creation - expensive operation"""
        import json
        
        # Log cache miss (this function only runs on cache miss)
        global _plot_cache_stats
        _plot_cache_stats['misses'] += 1
        logger.info(f"Cache miss - creating new plot for {region} ({time_range_display})")
        
        # Deserialize data
        plot_data = pd.read_json(plot_data_json)
        plot_data['settlementdate'] = pd.to_datetime(plot_data['settlementdate'])
        fuel_colors = json.loads(fuel_colors_json)
        fuel_types = list(fuel_types)
        
        # This is the expensive part - plot creation
        if has_negative_values:
            # Complex negative handling code
            return self._create_plot_with_negative_values(plot_data, fuel_types, fuel_colors, region, time_range_display)
        else:
            # Standard plot
            positive_fuel_types = [f for f in fuel_types if f != 'Transmission Exports']
            
            area_plot = plot_data.hvplot.area(
                x='settlementdate',
                y=positive_fuel_types,
                stacked=True,
                width=1200,
                height=300,
                ylabel='Generation (MW)',
                xlabel='',
                grid=True,
                legend='right',
                bgcolor='black',
                color=[fuel_colors.get(fuel, '#6272a4') for fuel in positive_fuel_types],
                alpha=0.8,
                hover=True,
                hover_tooltips=[('Fuel Type', '$name')],
                title=f'Generation by Fuel Type - {region} ({time_range_display}) | data:AEMO, design ITK'
            )
            
            area_plot = area_plot.opts(
                show_grid=False,
                bgcolor='black',
                xaxis=None,
                hooks=[self._get_datetime_formatter_hook()]
            )
            
            return area_plot
    
'''
    
    # Insert cached function before create_plot
    lines.insert(create_plot_idx, cached_plot_function)
    
    # Now modify create_plot to use caching
    # Find the plot creation section (around line 1280)
    for i in range(create_plot_idx, len(lines)):
        if 'if has_negative_values:' in lines[i]:
            # Replace the plot creation with cached version
            indent = '            '
            replacement = f'''{indent}# Create cache key
{indent}cache_key = create_plot_cache_key(
{indent}    data_shape=plot_data.shape,
{indent}    region=self.region,
{indent}    time_range=self._get_time_range_display(),
{indent}    fuel_types=tuple(fuel_types),
{indent}    has_negative=has_negative_values
{indent})
{indent}
{indent}# Try cached plot creation
{indent}try:
{indent}    area_plot = self._create_generation_plot_cached(
{indent}        cache_key=cache_key,
{indent}        plot_data_json=plot_data.to_json(date_format='iso'),
{indent}        fuel_types=tuple(fuel_types),
{indent}        fuel_colors_json=json.dumps(fuel_colors),
{indent}        has_negative_values=has_negative_values,
{indent}        region=self.region,
{indent}        time_range_display=self._get_time_range_display()
{indent}    )
{indent}    global _plot_cache_stats
{indent}    _plot_cache_stats['hits'] += 1
{indent}    total = _plot_cache_stats['hits'] + _plot_cache_stats['misses']
{indent}    hit_rate = (_plot_cache_stats['hits'] / total * 100) if total > 0 else 0
{indent}    logger.debug(f"Cache hit! Stats: hits={_plot_cache_stats['hits']}, misses={_plot_cache_stats['misses']}, rate={hit_rate:.1f}%")
{indent}except Exception as e:
{indent}    logger.error(f"Cache error, using direct creation: {{e}}")
{indent}    # Fallback to original logic
{indent}    if has_negative_values:
'''
            
            # Find the end of the plot creation block
            end_idx = i
            brace_count = 0
            for j in range(i, len(lines)):
                if '{' in lines[j]:
                    brace_count += lines[j].count('{')
                if '}' in lines[j]:
                    brace_count -= lines[j].count('}')
                if 'else:' in lines[j] and brace_count == 0:
                    end_idx = j
                    break
            
            # Replace the section
            lines[i] = replacement
            # Keep original logic as fallback
            
            break
    
    # Add import for json if not already present
    json_imported = any('import json' in line for line in lines[:import_end_idx])
    if not json_imported:
        lines.insert(import_end_idx - 1, 'import json\n')
    
    # Write the modified file
    with open(original_file, 'w') as f:
        f.writelines(lines)
    
    print(f"Successfully added caching to {original_file}")
    print(f"Backup saved as {backup_file}")
    print("\nTo enable/disable caching, set environment variable:")
    print("  export ENABLE_PN_CACHE=true   # Enable caching")
    print("  export ENABLE_PN_CACHE=false  # Disable caching")

if __name__ == "__main__":
    add_caching_to_gen_dash()