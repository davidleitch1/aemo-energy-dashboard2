"""
Price Analysis UI Components - User interface for flexible price analysis

This module provides Panel components for the Average Price Analysis tab,
including hierarchy selection and tabulator display.
"""

import pandas as pd
import panel as pn
from typing import Dict, List, Optional
import param
from datetime import datetime, timedelta

from .price_analysis import PriceAnalysisMotor
from ..shared.logging_config import get_logger
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER,
    FLEXOKI_BLACK,
    FLEXOKI_BASE,
    FLEXOKI_ACCENT,
)

logger = get_logger(__name__)

class PriceAnalysisUI(param.Parameterized):
    """UI component for price analysis with flexible aggregation"""
    
    # Parameters for reactive UI
    refresh_data = param.Boolean(default=False)
    start_date = param.Date(default=None)
    end_date = param.Date(default=None)
    time_range = param.Selector(
        default='All',
        objects=['1', '7', '30', 'All'],
        doc="Select time range to display"
    )
    
    # Grouping and column selection parameters
    selected_grouping = param.List(default=['Fuel'], bounds=(0, 3))
    selected_columns = param.List(default=['generation_mwh', 'total_revenue_dollars', 'average_price_per_mwh'])
    
    def __init__(self):
        super().__init__()
        self.motor = PriceAnalysisMotor()
        self.data_loaded = False
        self.current_data = pd.DataFrame()
        
        # Initialize the motor
        self._initialize_motor()
        
        # Create UI components
        self.grouping_checkboxes = None
        self.column_checkboxes = None
        self.apply_grouping_button = None
        self.tabulator_table = None
        self.tabulator_container = None  # Container for dynamic tabulator updates
        self.detail_table = None  # Table for DUID details
        self.detail_container = None  # Container for detail table
        self.status_text = None
        self.loading_spinner = None  # Loading indicator
        self.start_date_picker = None
        self.end_date_picker = None
        self.date_preset_buttons = None
        self.apply_date_button = None
        self.table_title = None  # Dynamic table title
        self.create_ui_components()
        
        # Track active date preset for visual feedback
        self.active_date_preset = None  # Track which preset is active (7, 30, None, or 'custom')
        
        logger.info("Price Analysis UI initialized")
    
    def _initialize_motor(self):
        """Initialize the calculation motor with data"""
        try:
            logger.info("Loading data into calculation motor...")
            # Use 30-minute data for full historical range when "All" is selected
            use_30min = (self.time_range == 'All')
            logger.info(f"Using {'30-minute' if use_30min else '5-minute'} data based on time range: {self.time_range}")
            
            if self.motor.load_data(use_30min_data=use_30min):
                # Don't call integrate_data here - it's now done on demand
                self.data_loaded = True
                logger.info("Data metadata loaded successfully")
            else:
                logger.error("Failed to load data")
        except Exception as e:
            logger.error(f"Error initializing motor: {e}")
            self.data_loaded = False
    
    def create_ui_components(self):
        """Create the UI components"""
        
        # Status indicator
        if self.data_loaded:
            status_msg = "✅ Data sources available"
            # Don't try to access integrated_data yet - it's loaded on demand
            if hasattr(self, 'motor') and self.motor.date_ranges:
                # Show available date ranges
                if 'generation' in self.motor.date_ranges:
                    gen_start = self.motor.date_ranges['generation']['start']
                    gen_end = self.motor.date_ranges['generation']['end']
                    status_msg += f" | Generation: {gen_start.strftime('%Y-%m-%d')} to {gen_end.strftime('%Y-%m-%d')}"
        else:
            status_msg = "❌ Failed to load data"
        
        self.status_text = pn.pane.Markdown(f"**Status:** {status_msg}")
        
        # Grouping and column selection controls
        if self.data_loaded:
            self._create_grouping_controls()
            self._create_column_controls()
            
            # Date range controls
            self._create_date_controls()
            
            # Refresh button
            self.refresh_button = pn.widgets.Button(
                name="Refresh Analysis",
                button_type="primary",
                width=150
            )
            self.refresh_button.on_click(self._on_refresh_click)
            
            # Create dynamic table title
            self.table_title = pn.pane.Markdown(self._get_table_title())
            
            # Create loading spinner
            self.loading_spinner = pn.indicators.LoadingSpinner(
                size=50, 
                value=False,
                color='primary',
                bgcolor='light',
                name='Loading data...'
            )
            
            # Don't calculate initial data - wait for user to apply filters
            # This avoids loading all data at startup
            self.tabulator_container = pn.Column(
                pn.pane.Markdown("**Click 'Update Analysis' to load data**"),
                sizing_mode='stretch_width',
                loading=False  # Panel's built-in loading parameter
            )
            
        else:
            self.grouping_checkboxes = pn.pane.Markdown("**Data loading failed - grouping controls unavailable**")
            self.column_checkboxes = pn.pane.Markdown("**Data loading failed - column controls unavailable**")
            self.tabulator_table = pn.pane.Markdown("**Cannot display table without data**")
            self.tabulator_container = pn.Column(self.tabulator_table, sizing_mode='stretch_width')
            self.detail_container = pn.Column(sizing_mode='stretch_width')
    
    def _create_date_controls(self):
        """Create date range selection controls"""
        try:
            # Get available date range
            available_start, available_end = self.motor.get_available_date_range()
            
            if available_start and available_end:
                start_dt = datetime.strptime(available_start, '%Y-%m-%d').date()
                end_dt = datetime.strptime(available_end, '%Y-%m-%d').date()
                
                # Date pickers
                self.start_date_picker = pn.widgets.DatePicker(
                    name="Start Date",
                    value=start_dt,
                    start=start_dt,
                    end=end_dt,
                    width=150
                )
                
                self.end_date_picker = pn.widgets.DatePicker(
                    name="End Date", 
                    value=end_dt,
                    start=start_dt,
                    end=end_dt,
                    width=150
                )
                
                # Time range selector with radio buttons (same as generation tab)
                self.time_range_widget = pn.widgets.RadioBoxGroup(
                    name="",  # Empty name since we add label separately
                    value=self.time_range,
                    options=['1', '7', '30', 'All'],
                    inline=True,  # Horizontal layout
                    width=250
                )
                self.time_range_widget.link(self, value='time_range')
                
                # Set initial active preset to "All Data"
                self.active_date_preset = None
                
                # Watch for changes to time_range and apply preset
                self.param.watch(self._on_time_range_change, 'time_range')
                
                # Apply button
                self.apply_date_button = pn.widgets.Button(
                    name="Apply Date Filter",
                    button_type="primary",
                    width=150
                )
                self.apply_date_button.on_click(self._on_apply_date_filter)
                
                logger.info(f"Date controls created for range: {available_start} to {available_end}")
                
            else:
                logger.warning("No date range available - creating disabled controls")
                self.start_date_picker = pn.pane.Markdown("**Date selection unavailable**")
                self.end_date_picker = pn.pane.Markdown("")
                self.apply_date_button = pn.pane.Markdown("")
                
        except Exception as e:
            logger.error(f"Error creating date controls: {e}")
            self.start_date_picker = pn.pane.Markdown("**Date controls error**")
            self.end_date_picker = pn.pane.Markdown("")
            self.apply_date_button = pn.pane.Markdown("")
    
    def _on_time_range_change(self, event):
        """Handle time range change from radio button group"""
        try:
            time_range = event.new
            logger.info(f"Time range changed to: {time_range}")
            
            # Reload data with appropriate resolution based on time range
            self._initialize_motor()
            
            # Map radio button values to preset days
            if time_range == '1':
                self._apply_date_preset(1)
            elif time_range == '7':
                self._apply_date_preset(7)
            elif time_range == '30':
                self._apply_date_preset(30)
            elif time_range == 'All':
                self._apply_date_preset(None)
                
        except Exception as e:
            logger.error(f"Error handling time range change: {e}")
    
    def _ensure_data_loaded(self, start_date=None, end_date=None):
        """Ensure data is loaded for the given date range"""
        try:
            if self.motor.integrated_data is None or start_date or end_date:
                # Need to load data
                logger.info(f"Loading data for date range: {start_date} to {end_date}")
                if not self.motor.integrate_data(start_date, end_date):
                    logger.error("Failed to integrate data")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error ensuring data loaded: {e}")
            return False
    
    def _get_table_title(self) -> str:
        """Generate dynamic table title with date information"""
        try:
            if self.data_loaded and hasattr(self.motor, 'integrated_data') and self.motor.integrated_data is not None and len(self.motor.integrated_data) > 0:
                start_date = self.motor.integrated_data['settlementdate'].min().strftime('%Y-%m-%d')
                end_date = self.motor.integrated_data['settlementdate'].max().strftime('%Y-%m-%d')
                days = (self.motor.integrated_data['settlementdate'].max() - self.motor.integrated_data['settlementdate'].min()).days
                
                if self.active_date_preset == 7:
                    return f"## Aggregated Results - Last 7 Days ({start_date} to {end_date})"
                elif self.active_date_preset == 30:
                    return f"## Aggregated Results - Last 30 Days ({start_date} to {end_date})"
                elif self.active_date_preset is None:
                    return f"## Aggregated Results - All Data ({start_date} to {end_date}, {days} days)"
                else:
                    return f"## Aggregated Results - Custom Range ({start_date} to {end_date}, {days} days)"
            else:
                return "## Aggregated Results"
        except Exception as e:
            logger.error(f"Error generating table title: {e}")
            return "## Aggregated Results"
    
    def _apply_date_preset(self, days: Optional[int]):
        """Apply a date preset (7 days, 30 days, or all data)"""
        try:
            available_start, available_end = self.motor.get_available_date_range()
            if not available_start or not available_end:
                return
                
            end_dt = datetime.strptime(available_end, '%Y-%m-%d').date()
            
            if days is None:
                # All data
                start_dt = datetime.strptime(available_start, '%Y-%m-%d').date()
            else:
                # Last N days
                start_dt = end_dt - timedelta(days=days-1)
                # Don't go before the earliest available date
                earliest_dt = datetime.strptime(available_start, '%Y-%m-%d').date()
                start_dt = max(start_dt, earliest_dt)
            
            # Update the date pickers
            self.start_date_picker.value = start_dt
            self.end_date_picker.value = end_dt
            
            # Update active preset tracking
            self.active_date_preset = days
            
            logger.info(f"Applied date preset: {days} days -> {start_dt} to {end_dt}")
            
        except Exception as e:
            logger.error(f"Error applying date preset: {e}")
    
    def _on_apply_date_filter(self, event):
        """Handle apply date filter button click"""
        try:
            start_date_str = self.start_date_picker.value.strftime('%Y-%m-%d') if self.start_date_picker.value else None
            end_date_str = self.end_date_picker.value.strftime('%Y-%m-%d') if self.end_date_picker.value else None
            
            logger.info(f"Applying date filter: {start_date_str} to {end_date_str}")
            
            # Load data with date filter
            if self.motor.integrate_data(start_date_str, end_date_str):
                self.data_loaded = True
                
                # Force complete rebuild of the table with new filtered data
                self._calculate_and_update_table()
                
                # Update status
                if len(self.motor.integrated_data) > 0:
                    filtered_start = self.motor.integrated_data['settlementdate'].min()
                    filtered_end = self.motor.integrated_data['settlementdate'].max()
                    filtered_days = (filtered_end - filtered_start).days
                    status_msg = f"✅ Date filter applied | Period: {filtered_start.strftime('%Y-%m-%d')} to {filtered_end.strftime('%Y-%m-%d')} ({filtered_days} days)"
                    status_msg += f" | Records: {len(self.motor.integrated_data):,}"
                else:
                    status_msg = "⚠️ No data found for selected date range"
                
                self.status_text.object = f"**Status:** {status_msg}"
                logger.info("Date filter applied successfully")
            else:
                logger.error("Failed to integrate data with date filter")
                
        except Exception as e:
            logger.error(f"Error applying date filter: {e}")
    
    def _create_grouping_controls(self):
        """Create improved grouping controls with category selection and item filters"""
        try:
            # Get unique values from data for filter options
            regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']  # Known regions
            # Use default fuel types since data isn't loaded yet
            fuels = ['Coal', 'Gas', 'Solar', 'Wind', 'Water', 'CCGT', 'OCGT', 'Distillate', 'Other']
            technologies = ['CCGT', 'OCGT', 'Steam', 'PV', 'Wind'] # Common tech types
            
            # Category selection: Which dimensions to group by
            self.category_selector = pn.widgets.CheckBoxGroup(
                name="Group by Categories:",
                value=['Fuel'],  # Default selection
                options=['Region', 'Fuel'],
                inline=True,
                width=400
            )
            
            # Region filters with shorter names and compact spacing
            self.region_filters = pn.widgets.CheckBoxGroup(
                name="Regions:",
                value=regions,  # Default: all regions
                options=regions,
                inline=False,  # Vertical layout within the column
                width=120,
                margin=(5, 0)  # Tighter spacing
            )
            
            # Fuel filters with shorter names and compact spacing
            self.fuel_filters = pn.widgets.CheckBoxGroup(
                name="Fuels:",
                value=fuels,  # Default: all fuels
                options=fuels,
                inline=False,  # Vertical layout within the column
                width=140,
                margin=(5, 0)  # Tighter spacing
            )
            
            # Uncheck All checkboxes (act like master switches)
            self.uncheck_all_regions_checkbox = pn.widgets.Checkbox(
                name="Uncheck All",
                value=False,
                width=100
            )
            self.uncheck_all_regions_checkbox.param.watch(self._on_uncheck_all_regions_change, 'value')
            
            self.uncheck_all_fuels_checkbox = pn.widgets.Checkbox(
                name="Uncheck All",
                value=False,
                width=100
            )
            self.uncheck_all_fuels_checkbox.param.watch(self._on_uncheck_all_fuels_change, 'value')
            
            # Unified update button for all changes
            self.update_analysis_button = pn.widgets.Button(
                name="Update Analysis",
                button_type="primary",
                width=150
            )
            self.update_analysis_button.on_click(self._on_update_analysis)
            
            logger.info("Improved grouping controls created successfully")
            
        except Exception as e:
            logger.error(f"Error creating grouping controls: {e}")
            self.category_selector = pn.pane.Markdown("**Grouping controls error**")
            self.region_filters = pn.pane.Markdown("")
            self.fuel_filters = pn.pane.Markdown("")
            self.uncheck_all_regions_checkbox = pn.pane.Markdown("")
            self.uncheck_all_fuels_checkbox = pn.pane.Markdown("")
            self.update_analysis_button = pn.pane.Markdown("")
    
    def _create_column_controls(self):
        """Create checkbox controls for column selection"""
        try:
            # Available display columns with shorter names - use list format like other working checkboxes
            column_display_names = [
                'Gen (GWh)', 'Rev ($M)', 'Price ($/MWh)', 'Util (%)', 
                'Cap (MW)', 'Station', 'Owner'
            ]
            
            self.column_checkboxes = pn.widgets.CheckBoxGroup(
                name="Columns:",
                value=column_display_names,  # All columns selected by default - use same list for value and options
                options=column_display_names,  # Use list directly like working region/fuel filters
                inline=False,  # Vertical layout within the column
                width=160,
                margin=(5, 0)  # Tighter spacing
            )
            
            # Uncheck All checkbox for columns
            self.uncheck_all_columns_checkbox = pn.widgets.Checkbox(
                name="Uncheck All",
                value=False,
                width=100
            )
            self.uncheck_all_columns_checkbox.param.watch(self._on_uncheck_all_columns_change, 'value')
            
            logger.info("Column controls created successfully")
            
        except Exception as e:
            logger.error(f"Error creating column controls: {e}")
            self.column_checkboxes = pn.pane.Markdown("**Column controls error**")
            self.uncheck_all_columns_checkbox = pn.pane.Markdown("")
    
    def _on_update_analysis(self, event):
        """Handle unified update analysis button click - combines date filtering and grouping"""
        try:
            # Show loading indicator immediately
            self._show_loading("Loading and integrating data...")
            
            # Step 1: Apply date filtering first (if date controls exist)
            if hasattr(self, 'start_date_picker') and hasattr(self.start_date_picker, 'value'):
                start_date_str = self.start_date_picker.value.strftime('%Y-%m-%d') if self.start_date_picker.value else None
                end_date_str = self.end_date_picker.value.strftime('%Y-%m-%d') if self.end_date_picker.value else None
                
                logger.info(f"Applying date filter: {start_date_str} to {end_date_str}")
                
                # Update loading message for data integration phase
                self._show_loading("Integrating data (this may take 30-60 seconds for large date ranges)...")
                
                # Load data with date filter
                if self.motor.integrate_data(start_date_str, end_date_str):
                    self.data_loaded = True
                    logger.info("Date filter applied successfully")
                else:
                    logger.error("Failed to integrate data with date filter")
                    # Show error and hide loading
                    error_msg = pn.pane.Markdown("**Error:** Failed to integrate data. Please check your date range.")
                    self.tabulator_container.clear()
                    self.tabulator_container.append(error_msg)
                    self._hide_loading()
                    return
            
            # Step 2: Apply grouping and column selections
            # Check if widgets are properly initialized
            if not hasattr(self.category_selector, 'value') or not hasattr(self.column_checkboxes, 'value'):
                logger.warning("Grouping controls not properly initialized")
                return
            
            # Get selected grouping categories
            selected_categories = self.category_selector.value if hasattr(self.category_selector, 'value') else ['Fuel']
            
            # Get filter selections
            selected_regions = self.region_filters.value if hasattr(self.region_filters, 'value') else []
            selected_fuels = self.fuel_filters.value if hasattr(self.fuel_filters, 'value') else []
            
            logger.info(f"Selected categories: {selected_categories}")
            logger.info(f"Region filters: {selected_regions}")
            logger.info(f"Fuel filters: {selected_fuels}")
            
            # Use selected categories as the grouping hierarchy
            self.selected_grouping = selected_categories if selected_categories else ['Fuel']  # Default fallback
            
            # Store filter selections for data filtering
            self.selected_region_filters = selected_regions
            self.selected_fuel_filters = selected_fuels
            if hasattr(self.column_checkboxes, 'value'):
                # Extract column names from tuples if needed
                raw_selection = self.column_checkboxes.value
                if raw_selection:
                    if isinstance(raw_selection[0], tuple):
                        # If selection contains tuples, extract just the column names (first element)
                        self.selected_columns = [item[0] if isinstance(item, tuple) else item for item in raw_selection]
                    else:
                        self.selected_columns = raw_selection
                else:
                    # If nothing selected, use default display names
                    self.selected_columns = ['Gen (GWh)', 'Rev ($M)', 'Price ($/MWh)']
            else:
                self.selected_columns = ['Gen (GWh)', 'Rev ($M)', 'Price ($/MWh)']  # Default fallback
            
            logger.info(f"Applied unified update - grouping: {self.selected_grouping}, columns: {self.selected_columns}")
            
            # Step 3: Rebuild the table with all selections
            self._calculate_and_update_table()
            
            # Step 4: Update status with current data state and mark as custom if dates were manually changed
            if len(self.motor.integrated_data) > 0:
                filtered_start = self.motor.integrated_data['settlementdate'].min()
                filtered_end = self.motor.integrated_data['settlementdate'].max()
                filtered_days = (filtered_end - filtered_start).days
                status_msg = f"✅ Analysis updated | Period: {filtered_start.strftime('%Y-%m-%d')} to {filtered_end.strftime('%Y-%m-%d')} ({filtered_days} days)"
                status_msg += f" | Records: {len(self.motor.integrated_data):,}"
                
                # Check if date range was manually changed (not matching any preset)
                if hasattr(self, 'start_date_picker') and hasattr(self.start_date_picker, 'value'):
                    current_start = self.start_date_picker.value
                    current_end = self.end_date_picker.value
                    
                    # Check if this matches any preset pattern
                    available_start, available_end = self.motor.get_available_date_range()
                    if available_start and available_end:
                        end_dt = datetime.strptime(available_end, '%Y-%m-%d').date()
                        
                        # Check against preset patterns
                        preset_7d_start = end_dt - timedelta(days=6)  # 7 days including today
                        preset_30d_start = end_dt - timedelta(days=29)  # 30 days including today
                        all_data_start = datetime.strptime(available_start, '%Y-%m-%d').date()
                        
                        if not ((current_start == preset_7d_start and current_end == end_dt) or
                                (current_start == preset_30d_start and current_end == end_dt) or
                                (current_start == all_data_start and current_end == end_dt)):
                            # This is a custom date range
                            self.active_date_preset = 'custom'
                            # Update radio button to reflect custom selection
                            self.time_range = 'All'  # Default to All when custom range is used
                
            else:
                status_msg = "⚠️ No data found for selected filters"
            
            self.status_text.object = f"**Status:** {status_msg}"
            
        except Exception as e:
            logger.error(f"Error in unified update analysis: {e}")
            # Show error and hide loading
            error_msg = pn.pane.Markdown(f"**Error during update:** {str(e)}")
            if self.tabulator_container:
                self.tabulator_container.clear()
                self.tabulator_container.append(error_msg)
            self._hide_loading()
    
    def _on_uncheck_all_regions_change(self, event):
        """Handle uncheck all regions checkbox change"""
        try:
            if event.new:  # If checkbox was just checked
                if hasattr(self, 'region_filters') and hasattr(self.region_filters, 'value'):
                    self.region_filters.value = []
                    logger.info("Unchecked all regions")
                # Reset checkbox to unchecked state (it's a toggle action)
                self.uncheck_all_regions_checkbox.value = False
        except Exception as e:
            logger.error(f"Error handling uncheck all regions: {e}")
    
    def _on_uncheck_all_fuels_change(self, event):
        """Handle uncheck all fuels checkbox change"""
        try:
            if event.new:  # If checkbox was just checked
                if hasattr(self, 'fuel_filters') and hasattr(self.fuel_filters, 'value'):
                    self.fuel_filters.value = []
                    logger.info("Unchecked all fuels")
                # Reset checkbox to unchecked state (it's a toggle action)
                self.uncheck_all_fuels_checkbox.value = False
        except Exception as e:
            logger.error(f"Error handling uncheck all fuels: {e}")
    
    def _on_uncheck_all_columns_change(self, event):
        """Handle uncheck all columns checkbox change"""
        try:
            if event.new:  # If checkbox was just checked
                if hasattr(self, 'column_checkboxes') and hasattr(self.column_checkboxes, 'value'):
                    self.column_checkboxes.value = []
                    logger.info("Unchecked all columns")
                # Reset checkbox to unchecked state (it's a toggle action)
                self.uncheck_all_columns_checkbox.value = False
        except Exception as e:
            logger.error(f"Error handling uncheck all columns: {e}")
    
    def _on_apply_grouping(self, event):
        """Handle apply grouping button click with new UI structure"""
        try:
            # Check if widgets are properly initialized
            if not hasattr(self.category_selector, 'value') or not hasattr(self.column_checkboxes, 'value'):
                logger.warning("Grouping controls not properly initialized")
                return
            
            # Get selected grouping categories
            selected_categories = self.category_selector.value if hasattr(self.category_selector, 'value') else ['Fuel']
            
            # Get filter selections
            selected_regions = self.region_filters.value if hasattr(self.region_filters, 'value') else []
            selected_fuels = self.fuel_filters.value if hasattr(self.fuel_filters, 'value') else []
            
            logger.info(f"Selected categories: {selected_categories}")
            logger.info(f"Region filters: {selected_regions}")
            logger.info(f"Fuel filters: {selected_fuels}")
            
            # Use selected categories as the grouping hierarchy
            self.selected_grouping = selected_categories if selected_categories else ['Fuel']  # Default fallback
            
            # Store filter selections for data filtering
            self.selected_region_filters = selected_regions
            self.selected_fuel_filters = selected_fuels
            if hasattr(self.column_checkboxes, 'value'):
                # Extract column names from tuples if needed
                raw_selection = self.column_checkboxes.value
                if raw_selection:
                    if isinstance(raw_selection[0], tuple):
                        # If selection contains tuples, extract just the column names (first element)
                        self.selected_columns = [item[0] if isinstance(item, tuple) else item for item in raw_selection]
                    else:
                        self.selected_columns = raw_selection
                else:
                    # If nothing selected, use default display names
                    self.selected_columns = ['Gen (GWh)', 'Rev ($M)', 'Price ($/MWh)']
            else:
                self.selected_columns = ['Gen (GWh)', 'Rev ($M)', 'Price ($/MWh)']  # Default fallback
            
            logger.info(f"Applied grouping: {self.selected_grouping}, columns: {self.selected_columns}")
            
            # Rebuild the table
            self._calculate_and_update_table()
            
        except Exception as e:
            logger.error(f"Error applying grouping: {e}")
    
    def _on_refresh_click(self, event):
        """Handle refresh button click"""
        logger.info("Refreshing analysis data...")
        self._initialize_motor()
        if self.data_loaded:
            self._calculate_and_update_table()
            # Update status
            overlap_start = self.motor.integrated_data['settlementdate'].min()
            overlap_end = self.motor.integrated_data['settlementdate'].max()
            overlap_days = (overlap_end - overlap_start).days
            status_msg = f"✅ Data refreshed successfully | Data period: {overlap_start.strftime('%Y-%m-%d')} to {overlap_end.strftime('%Y-%m-%d')} ({overlap_days} days)"
            status_msg += f" | Records: {len(self.motor.integrated_data):,}"
            self.status_text.object = f"**Status:** {status_msg}"
    
    def _show_loading(self, message: str = "Loading data..."):
        """Show loading indicator in the tabulator container"""
        if self.tabulator_container is not None:
            self.tabulator_container.loading = True
            # Also show a loading message
            loading_content = pn.Column(
                pn.pane.Markdown(f"### {message}"),
                pn.indicators.LoadingSpinner(size=50, value=True, color='primary'),
                align='center',
                sizing_mode='stretch_width'
            )
            self.tabulator_container.clear()
            self.tabulator_container.append(loading_content)
    
    def _hide_loading(self):
        """Hide loading indicator"""
        if self.tabulator_container is not None:
            self.tabulator_container.loading = False
    
    def _calculate_and_update_table(self):
        """Calculate aggregated data and update the table using dynamic grouping"""
        if not self.data_loaded:
            return
        
        # Show loading indicator
        self._show_loading("Calculating aggregated prices...")
        
        try:
            # Use dynamic grouping selection
            hierarchy_columns = self.selected_grouping if hasattr(self, 'selected_grouping') and self.selected_grouping else ['Fuel']
            
            # Map UI names to database column names
            column_name_mapping = {
                'Region': 'region',
                'Fuel': 'fuel_type',
                'duid': 'duid'
            }
            hierarchy_columns = [column_name_mapping.get(col, col) for col in hierarchy_columns]
            
            # Handle column selection - extract column names from tuples if needed
            if hasattr(self, 'selected_columns') and self.selected_columns:
                raw_columns = self.selected_columns
                if raw_columns and isinstance(raw_columns[0], tuple):
                    user_selected_columns = [item[0] if isinstance(item, tuple) else item for item in raw_columns]
                else:
                    user_selected_columns = raw_columns
                    
                # Map display names back to database column names for aggregated data
                column_mapping = {
                    'Gen (GWh)': 'generation_mwh',
                    'Rev ($M)': 'total_revenue_dollars', 
                    'Price ($/MWh)': 'average_price_per_mwh',
                    'Util (%)': 'capacity_utilization_pct',
                    'Cap (MW)': 'capacity_mw',
                    'Station': 'station_name',
                    'Owner': 'owner'
                }
                selected_columns = [column_mapping.get(col, col) for col in user_selected_columns]
            else:
                selected_columns = ['generation_mwh', 'total_revenue_dollars', 'average_price_per_mwh']
            
            logger.info(f"Calculating aggregations for grouping: {hierarchy_columns}")
            logger.info(f"Selected display columns: {selected_columns}")
            
            # Get filter selections if they exist
            region_filters = getattr(self, 'selected_region_filters', None)
            fuel_filters = getattr(self, 'selected_fuel_filters', None)
            
            # Calculate aggregated data with filters
            aggregated_data = self.motor.calculate_aggregated_prices(hierarchy_columns, region_filters, fuel_filters)
            
            # Sort the data according to the hierarchy order for proper display
            if not aggregated_data.empty and hierarchy_columns:
                # Sort by all hierarchy columns to ensure proper ordering
                try:
                    aggregated_data = aggregated_data.sort_values(hierarchy_columns)
                    logger.info(f"Data sorted by hierarchy: {hierarchy_columns}")
                except Exception as e:
                    logger.warning(f"Could not sort by all hierarchy columns: {e}")
                    # Fallback to sorting by available columns
                    available_sort_cols = [col for col in hierarchy_columns if col in aggregated_data.columns]
                    if available_sort_cols:
                        aggregated_data = aggregated_data.sort_values(available_sort_cols)
            
            if aggregated_data.empty:
                self.tabulator_table = pn.pane.Markdown("**No data available for selected grouping**")
                if self.tabulator_container is None:
                    self.tabulator_container = pn.Column(self.tabulator_table, sizing_mode='stretch_width')
                else:
                    self.tabulator_container.clear()
                    self.tabulator_container.append(self.tabulator_table)
                self._hide_loading()
                return
            
            # Filter data to only show selected columns (plus grouping columns)
            # Ensure selected columns actually exist in the data
            available_columns = set(aggregated_data.columns)
            valid_selected_columns = [col for col in selected_columns if col in available_columns]
            
            if not valid_selected_columns:
                # Fallback if no valid columns selected - use original column names
                valid_selected_columns = ['generation_mwh', 'total_revenue_dollars', 'average_price_per_mwh']
                valid_selected_columns = [col for col in valid_selected_columns if col in available_columns]
            
            display_columns = hierarchy_columns + valid_selected_columns
            filtered_data = aggregated_data[display_columns].copy()
            
            # Apply formatting to aggregated data for display
            if 'generation_mwh' in filtered_data.columns:
                # Convert MWh to GWh and apply smart rounding
                filtered_data['generation_mwh'] = (filtered_data['generation_mwh'] / 1000).apply(
                    lambda x: round(x, 0) if x >= 10 else round(x, 1)
                )
                
            if 'total_revenue_dollars' in filtered_data.columns:
                # Convert dollars to millions and apply smart rounding
                filtered_data['total_revenue_dollars'] = (filtered_data['total_revenue_dollars'] / 1_000_000).apply(
                    lambda x: round(x, 0) if x >= 10 else round(x, 1)
                )
                
            if 'average_price_per_mwh' in filtered_data.columns:
                # Apply smart rounding to price
                filtered_data['average_price_per_mwh'] = filtered_data['average_price_per_mwh'].apply(
                    lambda x: round(x, 0) if x >= 10 else round(x, 1)
                )
                
            if 'capacity_utilization_pct' in filtered_data.columns:
                # Apply smart rounding to utilization
                filtered_data['capacity_utilization_pct'] = filtered_data['capacity_utilization_pct'].apply(
                    lambda x: round(x, 0) if x >= 10 else round(x, 1)
                )
                
            if 'capacity_mw' in filtered_data.columns:
                # Apply smart rounding to capacity
                filtered_data['capacity_mw'] = filtered_data['capacity_mw'].apply(
                    lambda x: round(x, 0) if x >= 10 else round(x, 1)
                )
            
            logger.info(f"Available columns: {list(available_columns)}")
            logger.info(f"Valid selected columns: {valid_selected_columns}")
            logger.info(f"Display columns: {display_columns}")
            
            # Create hierarchical data that includes both totals and DUIDs for proper grouping
            # Pass the original column names to the hierarchical data method
            hierarchical_data = self.motor.create_hierarchical_data(hierarchy_columns, selected_columns, region_filters, fuel_filters)
            
            if not hierarchical_data.empty:
                logger.info(f"Hierarchical data shape: {hierarchical_data.shape}")
                logger.info(f"Hierarchical data columns: {list(hierarchical_data.columns)}")
                logger.info(f"Sample of hierarchical data:\n{hierarchical_data.head(10)}")
                
                # Check if hierarchical data actually contains individual DUIDs
                if 'duid' in hierarchical_data.columns:
                    unique_duids = hierarchical_data['duid'].nunique()
                    logger.info(f"Hierarchical data contains {unique_duids} unique DUIDs")
                    # Check if DUIDs are not empty/null
                    non_null_duids = hierarchical_data['duid'].notna().sum()
                    logger.info(f"Non-null DUID entries: {non_null_duids}")
                
                # Filter hierarchical data to only show user-selected columns
                # Map user-selected column names to the formatted column names in hierarchical data
                # Use the display names that were selected by the user in the checkbox
                user_selected = getattr(self, 'selected_columns', ['Gen (GWh)', 'Rev ($M)', 'Price ($/MWh)'])
                
                # Build list of columns to display: hierarchy + duid + user-selected formatted columns
                display_cols = hierarchy_columns + ['duid']
                
                # Add the formatted column names that correspond to user display selections
                formatted_column_mapping = {
                    'Gen (GWh)': 'generation_gwh',
                    'Rev ($M)': 'revenue_millions', 
                    'Price ($/MWh)': 'avg_price',
                    'Util (%)': 'capacity_utilization',
                    'Cap (MW)': 'capacity_mw',
                    'Station': 'station_name',
                    'Owner': 'owner'
                }
                
                for user_col in user_selected:
                    formatted_col = formatted_column_mapping.get(user_col, user_col)
                    if formatted_col in hierarchical_data.columns:
                        display_cols.append(formatted_col)
                
                # Filter the hierarchical data to only include display columns
                filtered_hierarchical_data = hierarchical_data[display_cols].copy()
                
                logger.info(f"Displaying columns: {display_cols}")
                logger.info(f"Available hierarchical columns: {list(hierarchical_data.columns)}")
                
                # Check if we have DUID data
                unique_duids = filtered_hierarchical_data['duid'].nunique() if 'duid' in filtered_hierarchical_data.columns else 0
                logger.info(f"Number of unique DUIDs in hierarchical data: {unique_duids}")
                
                # Use hierarchical data that includes individual DUIDs (Panel creates group totals automatically)
                # Make sure 'duid' is not in the groupby columns since it's the detail level
                groupby_cols = [col for col in hierarchy_columns if col != 'duid']
                logger.info(f"Groupby columns: {groupby_cols}")
                
                self.tabulator_table = pn.widgets.Tabulator(
                    value=filtered_hierarchical_data,
                    groupby=groupby_cols,  # Panel handles the multi-level grouping (excluding 'duid')
                    pagination=None,  # Disable pagination for scrolling
                    sizing_mode='stretch_width',
                    height=800,
                    show_index=False,
                    sortable=True,
                    selectable=1,
                    theme='simple',  # Simple theme works better with custom styling
                    stylesheets=[f"""
                        .tabulator {{
                            background-color: {FLEXOKI_PAPER} !important;
                            color: {FLEXOKI_BLACK} !important;
                            border: 1px solid {FLEXOKI_BASE[150]} !important;
                        }}
                        .tabulator-tableholder {{
                            background-color: {FLEXOKI_PAPER} !important;
                        }}
                        .tabulator-table {{
                            background-color: {FLEXOKI_PAPER} !important;
                        }}
                        .tabulator-header {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                            color: {FLEXOKI_BLACK} !important;
                            font-weight: bold;
                        }}
                        .tabulator-header .tabulator-col {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                            color: {FLEXOKI_BLACK} !important;
                            border-bottom: 2px solid {FLEXOKI_BASE[200]} !important;
                        }}
                        .tabulator-header .tabulator-col-content {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                        }}
                        .tabulator-row {{
                            background-color: {FLEXOKI_PAPER} !important;
                            color: {FLEXOKI_BLACK} !important;
                        }}
                        .tabulator-row:nth-child(even) {{
                            background-color: {FLEXOKI_BASE[50]} !important;
                        }}
                        .tabulator-row:hover {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                        }}
                        .tabulator-row.tabulator-selected {{
                            background-color: {FLEXOKI_BASE[150]} !important;
                        }}
                        .tabulator-cell {{
                            color: {FLEXOKI_BLACK} !important;
                            border-right: 1px solid {FLEXOKI_BASE[100]} !important;
                            background-color: inherit !important;
                        }}
                        .tabulator-group {{
                            background-color: {FLEXOKI_BASE[50]} !important;
                            color: {FLEXOKI_BLACK} !important;
                        }}
                        .tabulator-group-header {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                            color: {FLEXOKI_BLACK} !important;
                            font-weight: bold;
                        }}
                        .tabulator-footer {{
                            background-color: {FLEXOKI_BASE[50]} !important;
                            color: {FLEXOKI_BLACK} !important;
                        }}
                        .tabulator-page {{
                            background-color: {FLEXOKI_PAPER} !important;
                            color: {FLEXOKI_BLACK} !important;
                        }}
                        .tabulator-page.active {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                        }}
                    """],
                    configuration={
                        'groupStartOpen': True,  # Start with groups expanded to show DUIDs
                        'groupToggleElement': 'header',  # Make entire header clickable to toggle
                        'virtualDomBuffer': 300,  # Enable virtual scrolling for performance
                        'columnHeaderSortMulti': True,
                        'headerFilterPlaceholder': '',
                        'layout': 'fitColumns',
                        'responsiveLayout': 'collapse'
                    }
                )
            else:
                logger.warning("Hierarchical data is empty, falling back to aggregated data")
                # Fallback: Simple table without grouping
                self.tabulator_table = pn.widgets.Tabulator(
                    value=filtered_data,
                    pagination=None,  # Disable pagination for scrolling
                    sizing_mode='stretch_width',
                    height=800,
                    show_index=False,
                    sortable=True,
                    selectable=1,
                    theme='simple',  # Simple theme works better with custom styling
                    stylesheets=[f"""
                        .tabulator {{
                            background-color: {FLEXOKI_PAPER} !important;
                            color: {FLEXOKI_BLACK} !important;
                            border: 1px solid {FLEXOKI_BASE[150]} !important;
                        }}
                        .tabulator-tableholder {{
                            background-color: {FLEXOKI_PAPER} !important;
                        }}
                        .tabulator-table {{
                            background-color: {FLEXOKI_PAPER} !important;
                        }}
                        .tabulator-header {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                            color: {FLEXOKI_BLACK} !important;
                            font-weight: bold;
                        }}
                        .tabulator-header .tabulator-col {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                            color: {FLEXOKI_BLACK} !important;
                            border-bottom: 2px solid {FLEXOKI_BASE[200]} !important;
                        }}
                        .tabulator-header .tabulator-col-content {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                        }}
                        .tabulator-row {{
                            background-color: {FLEXOKI_PAPER} !important;
                            color: {FLEXOKI_BLACK} !important;
                        }}
                        .tabulator-row:nth-child(even) {{
                            background-color: {FLEXOKI_BASE[50]} !important;
                        }}
                        .tabulator-row:hover {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                        }}
                        .tabulator-row.tabulator-selected {{
                            background-color: {FLEXOKI_BASE[150]} !important;
                        }}
                        .tabulator-cell {{
                            color: {FLEXOKI_BLACK} !important;
                            border-right: 1px solid {FLEXOKI_BASE[100]} !important;
                            background-color: inherit !important;
                        }}
                        .tabulator-footer {{
                            background-color: {FLEXOKI_BASE[50]} !important;
                            color: {FLEXOKI_BLACK} !important;
                        }}
                        .tabulator-page {{
                            background-color: {FLEXOKI_PAPER} !important;
                            color: {FLEXOKI_BLACK} !important;
                        }}
                        .tabulator-page.active {{
                            background-color: {FLEXOKI_BASE[100]} !important;
                        }}
                    """],
                    configuration={
                        'virtualDomBuffer': 300,  # Enable virtual scrolling for performance
                        'initialSort': [  # Set initial sort order based on hierarchy
                            {'column': col, 'dir': 'asc'} for col in hierarchy_columns
                        ] if hierarchy_columns else []
                    }
                )
            
            # Note: Individual DUIDs are now included in the hierarchical table above
            # Users can click on group headers to expand and see the DUIDs
            if self.detail_container is None:
                self.detail_container = pn.Column(sizing_mode='stretch_width')
            else:
                self.detail_container.clear()
            
            # Create or update the tabulator container
            if self.tabulator_container is None:
                self.tabulator_container = pn.Column(self.tabulator_table, sizing_mode='stretch_width')
            else:
                # Update the container with the new tabulator
                self.tabulator_container.clear()
                self.tabulator_container.append(self.tabulator_table)
            
            logger.info(f"Table updated with hierarchical data (aggregated totals + individual DUIDs)")
            
            # Update the table title with current data information
            if hasattr(self, 'table_title') and self.table_title is not None:
                self.table_title.object = self._get_table_title()
            
            # Hide loading indicator after successful update
            self._hide_loading()
            
        except Exception as e:
            logger.error(f"Error calculating aggregations: {e}")
            error_msg = pn.pane.Markdown(f"**Error calculating data:** {e}")
            self.tabulator_table = error_msg
            if self.tabulator_container is None:
                self.tabulator_container = pn.Column(error_msg, sizing_mode='stretch_width')
            else:
                self.tabulator_container.clear()
                self.tabulator_container.append(error_msg)
            
            # Hide loading indicator after error
            self._hide_loading()
    
    def _get_tabulator_columns(self, hierarchy_columns: List[str]) -> Dict:
        """Configure tabulator columns based on hierarchy"""
        
        # Base columns for hierarchy - ensure consistent ordering and visibility
        columns = {}
        
        # Always show hierarchy columns in a logical order regardless of selected hierarchy
        hierarchy_order = ['Region', 'Fuel', 'duid']  # Logical display order
        
        for i, col in enumerate(hierarchy_columns):
            # Determine if this is the primary grouping column (first in hierarchy)
            is_primary = (i == 0)
            
            if col == 'duid':
                columns[col] = {
                    'title': 'DUID', 
                    'width': 120, 
                    'frozen': is_primary,
                    'headerSort': True
                }
            elif col == 'Region':
                columns[col] = {
                    'title': 'Region', 
                    'width': 80, 
                    'frozen': is_primary,
                    'headerSort': True
                }
            elif col == 'Fuel':
                columns[col] = {
                    'title': 'Fuel Type', 
                    'width': 100, 
                    'frozen': is_primary,
                    'headerSort': True
                }
            else:
                columns[col] = {
                    'title': col.title(), 
                    'width': 100, 
                    'frozen': is_primary,
                    'headerSort': True
                }
        
        # Performance columns
        columns.update({
            'generation_mwh': {
                'title': 'Generation (MWh)', 
                'width': 130,
                'formatter': 'money',
                'formatterParams': {'precision': 1, 'symbol': '', 'symbolAfter': ' MWh'}
            },
            'total_revenue_dollars': {
                'title': 'Total Revenue ($)', 
                'width': 140,
                'formatter': 'money',
                'formatterParams': {'precision': 0, 'symbol': '$', 'thousand': ','}
            },
            'average_price_per_mwh': {
                'title': 'Avg Price ($/MWh)', 
                'width': 140,
                'formatter': 'money',
                'formatterParams': {'precision': 2, 'symbol': '$', 'symbolAfter': '/MWh'}
            },
            'capacity_mw': {
                'title': 'Capacity (MW)', 
                'width': 120,
                'formatter': 'money',
                'formatterParams': {'precision': 0, 'symbol': '', 'symbolAfter': ' MW'}
            },
            'capacity_utilization_pct': {
                'title': 'Capacity Utilization (%)', 
                'width': 150,
                'formatter': 'money',
                'formatterParams': {'precision': 1, 'symbol': '', 'symbolAfter': '%'}
            },
            'record_count': {
                'title': 'Data Points', 
                'width': 100,
                'formatter': 'money',
                'formatterParams': {'precision': 0, 'thousand': ','}
            },
            'start_date': {
                'title': 'Start Date', 
                'width': 100,
                'formatter': 'datetime',
                'formatterParams': {'outputFormat': 'YYYY-MM-DD'}
            },
            'end_date': {
                'title': 'End Date', 
                'width': 100,
                'formatter': 'datetime',
                'formatterParams': {'outputFormat': 'YYYY-MM-DD'}
            }
        })
        
        return columns
    
    def create_layout(self) -> pn.layout.Tabs:
        """Create the complete UI layout"""
        
        if not self.data_loaded:
            return pn.Column(
                "# Average Price Analysis",
                self.status_text if self.status_text else pn.pane.Markdown("**Status:** Checking..."),
                pn.pane.Markdown("**❌ Cannot load price analysis - please check data files**"),
                sizing_mode='stretch_width'
            )
        
        # Create date range panel
        if hasattr(self, 'start_date_picker') and hasattr(self.start_date_picker, 'value'):
            time_range_selector = pn.Column(
                pn.pane.HTML("<div style='color: #aaa; font-size: 11px; margin-bottom: 4px;'>Days</div>"),
                self.time_range_widget,
                width=250,
                margin=(5, 0)
            )
            
            date_range_panel = pn.Column(
                "### Date Range",
                pn.Row(
                    self.start_date_picker,
                    self.end_date_picker,
                    sizing_mode='stretch_width'
                ),
                pn.pane.Markdown("**Quick Presets:**"),
                time_range_selector,
                width=350
            )
        else:
            date_range_panel = pn.pane.Markdown("**Date range controls unavailable**")
        
        # Create compact horizontal filters panel
        if hasattr(self.category_selector, 'value'):
            # Region control section - compact vertical list
            region_section = pn.Column(
                "**Regions:**",
                self.uncheck_all_regions_checkbox,
                self.region_filters,
                width=120,
                margin=(0, 5)
            )
            
            # Fuel control section - compact vertical list
            fuel_section = pn.Column(
                "**Fuels:**",
                self.uncheck_all_fuels_checkbox,
                self.fuel_filters,
                width=140,
                margin=(0, 5)
            )
            
            # Column control section - compact vertical list
            if hasattr(self.column_checkboxes, 'value'):
                column_section = pn.Column(
                    "**Columns:**",
                    self.uncheck_all_columns_checkbox,
                    self.column_checkboxes,
                    width=160,
                    margin=(0, 5)
                )
            else:
                column_section = pn.Column(
                    "**Columns:**",
                    self.column_checkboxes,  # Error message
                    width=160,
                    margin=(0, 5)
                )
            
            # Compact horizontal filters panel
            filters_panel = pn.Column(
                "### Grouping & Filters",
                self.category_selector,
                pn.Row(
                    region_section,
                    pn.Spacer(width=10),
                    fuel_section,
                    pn.Spacer(width=10),
                    column_section,
                    sizing_mode='fixed'
                ),
                width=470  # Much more compact total width
            )
        else:
            filters_panel = pn.Column(
                "### Grouping & Filters",
                self.category_selector,  # This will be the error message
                width=470
            )
        
        # Create actions panel with unified update button only
        actions_panel = pn.Column(
            "### Actions",
            self.update_analysis_button,
            width=150
        )
        
        # Create info panel
        info_panel = pn.pane.Markdown("""
        **About Average Price Analysis:**
        
        This analysis calculates weighted average electricity prices by aggregating:
        - **Revenue**: Generation (MW) × Price ($/MWh) × 5-minute intervals
        - **Average Price**: Total Revenue ÷ Total Generation (MWh)
        - **Capacity Factor**: Generation ÷ (Capacity × Hours) × 100%
        
        Choose different aggregation hierarchies to explore the data from different perspectives.
        """, width=400)
        
        # Main content - table back to the right side with compact controls on left
        main_content = pn.Column(
            "# Average Price Analysis",
            # Status and table title on same row
            pn.Row(
                self.status_text,
                pn.Spacer(width=50),
                self.table_title if hasattr(self, 'table_title') and self.table_title is not None else pn.pane.Markdown("## Aggregated Results"),
                sizing_mode='stretch_width'
            ),
            pn.Spacer(height=15),
            # Main content row - controls on left, table on right
            pn.Row(
                # Left side - compact controls
                pn.Column(
                    "## Controls",
                    date_range_panel,
                    pn.Spacer(height=10),
                    actions_panel,
                    pn.Spacer(height=15),
                    filters_panel,
                    pn.Spacer(height=15),
                    info_panel,
                    width=550  # Fixed width for controls
                ),
                pn.Spacer(width=20),
                # Right side - table with full remaining width
                pn.Column(
                    self.tabulator_container,
                    pn.Spacer(height=20),
                    self.detail_container,
                    sizing_mode='stretch_width'
                ),
                sizing_mode='stretch_width'
            ),
            sizing_mode='stretch_width'
        )
        
        return main_content

# Factory function for easy integration
def create_price_analysis_tab() -> pn.layout.Column:
    """Create a price analysis tab for integration into the dashboard"""
    ui = PriceAnalysisUI()
    return ui.create_layout()