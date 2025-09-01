#!/usr/bin/env python3
"""
Fix for Midnight Display Freeze Bug

This script contains the fix that forces Panel components to recreate
when the date range changes at midnight.

The fix involves:
1. Detecting when date range changes (not just dates)
2. Forcing component recreation when range changes
3. Using param.trigger() to ensure display updates
"""

import logging

logger = logging.getLogger(__name__)

class MidnightDisplayFix:
    """
    Mixin class that provides the midnight display fix functionality.
    This can be added to dashboard components that need to handle midnight rollovers.
    """
    
    def __init__(self):
        # Track the last date range to detect changes
        self._last_date_range = None
        self._components_to_refresh = []
        
    def register_component_for_refresh(self, component_name, pane):
        """Register a Panel pane that needs refreshing at midnight"""
        self._components_to_refresh.append({
            'name': component_name,
            'pane': pane
        })
        
    def check_date_range_change(self, start_date, end_date):
        """
        Check if the date range has changed since last update.
        Returns True if range changed (requiring component recreation).
        """
        current_range = (start_date, end_date)
        
        if self._last_date_range is None:
            self._last_date_range = current_range
            return False
            
        range_changed = self._last_date_range != current_range
        
        if range_changed:
            logger.info(f"Date range changed: {self._last_date_range} → {current_range}")
            self._last_date_range = current_range
            
        return range_changed
        
    def force_component_refresh(self):
        """
        Force all registered Panel components to refresh their display.
        This is the key fix - it ensures Panel updates the display.
        """
        logger.info("Forcing component refresh due to date range change")
        
        for component in self._components_to_refresh:
            try:
                pane = component['pane']
                name = component['name']
                
                # Method 1: Trigger param update to force refresh
                if hasattr(pane, 'param') and hasattr(pane.param, 'trigger'):
                    pane.param.trigger('object')
                    logger.info(f"Triggered refresh for {name} using param.trigger")
                    
                # Method 2: If pane has object property, reassign it
                elif hasattr(pane, 'object'):
                    # Force reassignment to trigger update
                    current_object = pane.object
                    pane.object = None  # Clear first
                    pane.object = current_object  # Reassign
                    logger.info(f"Forced object reassignment for {name}")
                    
            except Exception as e:
                logger.error(f"Error refreshing component {name}: {e}")
                
    def create_plot_with_refresh(self, data, plot_func, pane_name):
        """
        Create a plot and its pane with automatic refresh capability.
        
        Args:
            data: The data to plot
            plot_func: Function that creates the plot from data
            pane_name: Name for this pane (for logging)
            
        Returns:
            The Panel pane containing the plot
        """
        import panel as pn
        
        # Create the plot
        plot = plot_func(data)
        
        # Create the pane
        pane = pn.pane.HoloViews(plot)
        
        # Register for refresh
        self.register_component_for_refresh(pane_name, pane)
        
        return pane


def apply_midnight_fix_to_update_loop(dashboard_instance):
    """
    Apply the midnight display fix to a dashboard's update loop.
    This wraps the existing update logic with date range checking.
    """
    
    # Store original update method
    original_update = dashboard_instance.update_plot
    
    # Create fix instance
    fix = MidnightDisplayFix()
    
    def wrapped_update():
        """Enhanced update that handles midnight properly"""
        
        # Get current date range
        start_date = dashboard_instance.start_date
        end_date = dashboard_instance.end_date
        
        # Check if date range changed
        if fix.check_date_range_change(start_date, end_date):
            logger.info("Date range change detected - forcing component refresh")
            
            # First, update the data as normal
            original_update()
            
            # Then force Panel components to refresh
            fix.force_component_refresh()
            
            # If dashboard has specific panes, refresh them
            if hasattr(dashboard_instance, 'refresh_all_panes'):
                dashboard_instance.refresh_all_panes()
                
        else:
            # Normal update when date range hasn't changed
            original_update()
    
    # Replace update method
    dashboard_instance.update_plot = wrapped_update
    
    # Attach fix instance for component registration
    dashboard_instance._midnight_fix = fix
    
    logger.info("Midnight display fix applied to dashboard")
    return dashboard_instance


def enhanced_auto_update_loop(self):
    """
    Enhanced auto_update_loop that properly handles midnight rollovers.
    This replaces the existing auto_update_loop in gen_dash.py
    """
    import asyncio
    from datetime import datetime
    
    async def auto_update_loop():
        """Main update loop with midnight display fix"""
        
        # Track last date range for change detection
        last_date_range = None
        
        while True:
            try:
                # FIX for midnight rollover bug: Refresh dates FIRST
                if self.time_range in ['1', '7', '30']:
                    old_start = self.start_date
                    old_end = self.end_date
                    self._update_date_range_from_preset()
                    new_start = self.start_date
                    new_end = self.end_date
                    
                    # Check if date RANGE changed (not just dates)
                    old_range = (old_start, old_end)
                    new_range = (new_start, new_end)
                    
                    if old_range != new_range and last_date_range is not None:
                        self.logger.info(f"Date range changed: {old_range} → {new_range}")
                        
                        # CRITICAL FIX: Force Panel component recreation
                        if hasattr(self, 'recreate_all_plots'):
                            self.logger.info("Recreating all plots due to date range change")
                            self.recreate_all_plots()
                        else:
                            # Fallback: trigger refresh on all panes
                            self.logger.info("Forcing component refresh")
                            self.force_panel_refresh()
                    
                    last_date_range = new_range
                
                # Normal update
                self.update_plot()
                
                # Also update loading indicator
                if hasattr(self, 'loading_indicator'):
                    self.loading_indicator.object = self._create_loading_indicator()
                
            except Exception as e:
                self.logger.error(f"Error in auto-update loop: {e}")
            
            # Wait before next update
            await asyncio.sleep(self.update_interval)
    
    return auto_update_loop


def create_recreate_all_plots_method(dashboard_instance):
    """
    Create a method that recreates all plot objects.
    This is the nuclear option but guarantees display updates.
    """
    
    def recreate_all_plots(self):
        """Recreate all plot objects to force display update"""
        import panel as pn
        
        try:
            # List of plot attributes to recreate
            plot_attrs = [
                'generation_plot_pane',
                'price_plot_pane', 
                'renewable_gauge_pane',
                'transmission_plot_pane'
            ]
            
            for attr_name in plot_attrs:
                if hasattr(self, attr_name):
                    pane = getattr(self, attr_name)
                    
                    if isinstance(pane, pn.pane.HoloViews):
                        # Get current plot object
                        current_plot = pane.object
                        
                        # Force recreation by clearing and reassigning
                        pane.object = None
                        pane.object = current_plot
                        
                        self.logger.info(f"Recreated {attr_name}")
                        
        except Exception as e:
            self.logger.error(f"Error recreating plots: {e}")
    
    # Attach method to instance
    dashboard_instance.recreate_all_plots = recreate_all_plots.__get__(dashboard_instance)


def create_force_panel_refresh_method(dashboard_instance):
    """
    Create a method that forces Panel to refresh all components.
    Uses param.trigger() which is Panel's recommended way.
    """
    
    def force_panel_refresh(self):
        """Force Panel to refresh all components"""
        
        # Find all Panel panes and trigger refresh
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            
            # Check if it's a Panel pane
            if hasattr(attr, 'param') and hasattr(attr, 'object'):
                try:
                    # Trigger refresh
                    attr.param.trigger('object')
                    self.logger.info(f"Triggered refresh for {attr_name}")
                except Exception as e:
                    self.logger.debug(f"Could not trigger {attr_name}: {e}")
                    
    # Attach method to instance
    dashboard_instance.force_panel_refresh = force_panel_refresh.__get__(dashboard_instance)


# Example usage in gen_dash.py:
"""
# In EnergyDashboard.__init__():
from fix_midnight_display_freeze import (
    create_recreate_all_plots_method,
    create_force_panel_refresh_method
)

# Add the new methods
create_recreate_all_plots_method(self)
create_force_panel_refresh_method(self)

# Then in auto_update_loop, after detecting date range change:
if old_range != new_range:
    self.recreate_all_plots()  # or self.force_panel_refresh()
"""