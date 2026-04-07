"""
Insights tab for AEMO Energy Dashboard
Based on Prices tab structure but with custom content
"""
import pandas as pd
import numpy as np
import panel as pn
import plotly.graph_objects as go
from datetime import datetime, time, timedelta
from typing import Optional, List

from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.adapter_selector import load_price_data
from aemo_dashboard.shared.adapter_selector import load_generation_data
from aemo_dashboard.shared.config import Config
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

logger = get_logger(__name__)

# Flexoki theme constants
FLEXOKI_PAPER = '#FFFCF0'
FLEXOKI_BORDER = '#B7B5AC'

# Optional LOESS import
try:
    from statsmodels.nonparametric.smoothers_lowess import lowess
    HAS_LOESS = True
except ImportError:
    HAS_LOESS = False
    logger.warning("statsmodels not available, LOESS smoothing disabled")

class InsightsTab:
    """Insights analysis tab with dynamic content and price controls"""
    
    def __init__(self):
        """Initialize the batteries tab"""
        # Force left-align for CheckBoxGroup widgets inside scroll containers
        pn.config.raw_css.append("""
            .bk-CheckboxGroup { width: 100% !important; margin-left: 0 !important; margin-right: auto !important; }
        """)
        # Initialize config
        self.config = Config()
        
        # Initialize generation query manager for efficient data loading
        self.query_manager = GenerationQueryManager()
        
        # Load battery information
        self._load_battery_info()
        
        # Initialize components
        self._setup_controls()
        self._setup_content_area()
        self._updating_batteries = False  # Guard flag for checkbox callbacks
        self._updating_regions = False
        self._updating_owners = False
        self._setup_region_controls()
    
    def _load_battery_info(self):
        """Load battery storage information from gen_info"""
        try:
            import pickle
            with open(self.config.gen_info_file, 'rb') as f:
                gen_info = pickle.load(f)
            
            # Filter for battery storage
            self.battery_info = gen_info[gen_info['Fuel'] == 'Battery Storage'].copy()
            
            # Group batteries by region for easy access
            self.batteries_by_region = {}
            for region in self.battery_info['Region'].unique():
                region_batteries = self.battery_info[self.battery_info['Region'] == region]
                # Create list of tuples (display_name, duid)
                battery_list = []
                for _, battery in region_batteries.iterrows():
                    capacity_mw = battery.get('Capacity(MW)', battery.get('Reg Cap (MW)', 0))
                    storage_mwh = battery.get('Storage(MWh)', 0)
                    # Calculate duration in hours
                    duration = storage_mwh / capacity_mw if capacity_mw > 0 else 0
                    display_name = f"{battery['Site Name']} ({battery['DUID']}) - {capacity_mw:.0f} MW / {storage_mwh:.0f} MWh ({duration:.1f}h)"
                    battery_list.append((display_name, battery['DUID']))
                # Sort by MW capacity descending
                battery_list.sort(key=lambda x: -self.battery_info[self.battery_info['DUID'] == x[1]]['Capacity(MW)'].values[0])
                self.batteries_by_region[region] = battery_list
            
            logger.info(f"Loaded {len(self.battery_info)} battery storage units")
            
        except Exception as e:
            logger.error(f"Error loading battery info: {e}")
            self.battery_info = pd.DataFrame()
            self.batteries_by_region = {}
        
    def _setup_controls(self):
        """Set up all control widgets (same as Prices tab)"""
        # Mapping dicts: compact label -> original value used in callbacks
        self._preset_map = {
            '1d': '1 day', '7d': '7 days', '30d': '30 days',
            '90d': '90 days', '1y': '1 year', 'All': 'All data'
        }
        self._freq_map = {
            '5m': '5 min', '1h': '1 hour', 'D': 'Daily',
            'M': 'Monthly', 'Q': 'Quarterly', 'Y': 'Yearly'
        }

        # Date preset toggle buttons (horizontal, compact labels)
        self.date_presets = pn.widgets.RadioButtonGroup(
            name='',
            options=list(self._preset_map.keys()),
            value='30d',
            button_type='default'
        )

        # Date pickers
        default_end = pd.Timestamp.now().date()
        default_start = default_end - pd.Timedelta(days=30)

        self.start_date_picker = pn.widgets.DatePicker(
            name='Start Date',
            value=default_start,
            width=120
        )

        self.end_date_picker = pn.widgets.DatePicker(
            name='End Date',
            value=default_end,
            width=120
        )

        # Region radio button group
        # Define region colors - Flexoki Light compatible
        self.region_colors = {
            'NSW1': '#879A39',  # Flexoki Green
            'QLD1': '#BC5215',  # Flexoki Orange
            'SA1': '#CE5D97',   # Flexoki Magenta
            'TAS1': '#3AA99F',  # Flexoki Cyan
            'VIC1': '#8B7EC8',  # Flexoki Purple
            'NEM': '#100F0F'    # Flexoki Black for NEM (all regions)
        }

        # Create inline RadioBoxGroup for regions
        self.region_selector = pn.widgets.RadioBoxGroup(
            name='',
            value='NEM',  # Default to NEM
            options=['NEM', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'],
            inline=True,
            align='start',
            margin=(0, 0, 0, 0)
        )


        # Aggregate level toggle buttons (horizontal, compact labels)
        self.aggregate_selector = pn.widgets.RadioButtonGroup(
            name='',
            value='1h',
            options=list(self._freq_map.keys()),
            button_type='default'
        )

        # Battery metric selector for lollipop chart
        self.metric_selector = pn.widgets.Select(
            name='Battery Metric',
            value='Discharge Revenue',
            options=[
                'Discharge Revenue',
                'Charge Cost',
                'Discharge Price',
                'Charge Price',
                'Discharge Energy',
                'Charge Energy',
                'Price Spread'
            ],
            width=180
        )

        # Update button for loading battery analysis
        self.update_button = pn.widgets.Button(
            name='\u25cf Update Analysis',
            button_type='success',
            width=160
        )
        
        # Set up callbacks
        self._setup_callbacks()
        
    def _setup_content_area(self):
        """Set up the content area for battery analysis"""
        # Battery analysis placeholder - now replaced with lollipop chart
        # Use a generic pane that can hold either HoloViews or Matplotlib objects
        self.battery_content_pane = pn.Column(
            sizing_mode='stretch_width',
            height=500
        )
        
        # Info pane for showing summary statistics
        self.battery_info_pane = pn.pane.HTML(
            """
            <div style="
                background-color: #FFFCF0;
                border: 2px solid #B7B5AC;
                border-radius: 10px;
                padding: 15px;
                margin: 10px;
                color: #100F0F;
            ">
                <p>Click 'Update Battery Analysis' to generate the battery performance chart.</p>
            </div>
            """,
            sizing_mode='stretch_width'
        )
        
        # Placeholder for future dynamic content
        self.dynamic_content_pane = pn.pane.Markdown(
            "",
            width=600
        )
    
    def _setup_region_controls(self):
        """Set up controls for Region aggregation subtab"""
        # Region checkboxes (multi-select replaces RadioButtonGroup)
        self.region_checkboxes = pn.widgets.CheckBoxGroup(
            name='',
            options=['NSW1', 'QLD1', 'SA1', 'VIC1'],
            value=['NSW1', 'QLD1', 'SA1', 'VIC1'],
            inline=False
        )
        self.region_select_all = pn.widgets.Checkbox(
            name='Select All', value=True, width=100
        )

        # Owner checkboxes (replaces MultiChoice dropdown)
        _left_justify_css = [
            ':host { width: 100% !important; display: block !important; text-align: left !important; }',
            '.bk-input-group { text-align: left !important; width: 100% !important; align-items: start !important; }',
            '.bk-input-group label { padding-left: 2px; margin-left: 0; text-align: left; }',
        ]
        self.owner_checkboxes = pn.widgets.CheckBoxGroup(
            name='', options=[], value=[], inline=False,
            sizing_mode='stretch_width', align='start',
            stylesheets=_left_justify_css
        )
        self.owner_select_all = pn.widgets.Checkbox(
            name='Select All', value=True, width=200
        )
        _col_left_css = [
            '* { align-items: start !important; justify-items: start !important; }',
        ]
        self.owner_scroll = pn.Column(
            self.owner_checkboxes, scroll=True, height=250,
            width=250,
            align='start', stylesheets=_col_left_css
        )

        # Battery checkboxes (scrollable, stretch width)
        self.battery_select_all = pn.widgets.Checkbox(
            name='Select All', value=True
        )
        self.region_battery_checkboxes = pn.widgets.CheckBoxGroup(
            name='', options={}, value=[], inline=False,
            sizing_mode='stretch_width', align='start',
            stylesheets=_left_justify_css
        )
        self.region_battery_scroll = pn.Column(
            self.region_battery_checkboxes,
            scroll=True, height=250,
            sizing_mode='stretch_width',
            align='start', stylesheets=_col_left_css
        )

        # Date preset buttons (horizontal row)
        self.region_date_presets = pn.widgets.RadioButtonGroup(
            name='',
            options=['1d', '7d', '30d', '90d', '1yr', 'All'],
            value='30d',
            button_type='default'
        )

        # Date pickers
        default_end = pd.Timestamp.now().date()
        default_start = default_end - pd.Timedelta(days=30)

        self.region_start_date = pn.widgets.DatePicker(
            name='Start Date',
            value=default_start,
            width=120
        )

        self.region_end_date = pn.widgets.DatePicker(
            name='End Date',
            value=default_end,
            width=120
        )

        # Frequency selector (horizontal toggle)
        self.region_frequency = pn.widgets.RadioButtonGroup(
            name='',
            value='1h',
            options=['5m', '1h'],
            button_type='default',
            button_style='outline'
        )

        # Log scale checkbox
        self.region_log_scale = pn.widgets.Checkbox(
            name='Log Scale (Price)',
            value=False,
            width=120
        )

        # Analyze button
        self.region_analyze_button = pn.widgets.Button(
            name='\u25cf Analyze Fleet',
            button_type='success',
            width=140
        )

        # Results panes
        self.region_info_pane = pn.pane.HTML(
            """
            <div style="background-color: #FFFCF0; border: 1px solid #B7B5AC; padding: 10px; border-radius: 5px;">
                <p style="color: #100F0F;">Select a region and click Analyze to view fleet-level battery metrics.</p>
            </div>
            """,
            sizing_mode='stretch_width'
        )

        self.region_chart_pane = pn.pane.Plotly(
            object=None,
            sizing_mode='stretch_width',
            height=600
        )

        # Time of Day sub-tab panes
        self.region_tod_chart_pane = pn.pane.Plotly(
            object=None,
            sizing_mode='stretch_width',
            height=500
        )
        self.region_tod_info_pane = pn.pane.HTML(
            """
            <div style="background-color: #FFFCF0; border: 1px solid #B7B5AC; padding: 10px; border-radius: 5px;">
                <p style="color: #100F0F;">Click <strong>Analyze Region</strong> to view hourly battery profiles.</p>
            </div>
            """,
            sizing_mode='stretch_width'
        )

        # Callbacks
        def update_region_date_range(event):
            """Update date range based on preset selection"""
            preset_days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90, '1yr': 365, 'All': None}
            days = preset_days.get(event.new)
            current_end = self.region_end_date.value
            if days is None:
                new_start = pd.Timestamp('2020-01-01').date()
            else:
                new_start = current_end - pd.Timedelta(days=days)
            self.region_start_date.value = new_start

        def analyze_region(event):
            """Analyze all batteries in selected regions"""
            selected_regions = self.region_checkboxes.value
            start_date = self.region_start_date.value
            end_date = self.region_end_date.value
            frequency = self.region_frequency.value

            if not selected_regions:
                self.region_info_pane.object = """
                <div style="background-color: #FFFCF0; border: 1px solid #B7B5AC; padding: 10px; border-radius: 5px;">
                    <p style="color: #AF3029;">No regions selected. Check at least one region.</p>
                </div>
                """
                self.region_chart_pane.object = None
                return

            # Get selected batteries from checkboxes
            fleet_duids = self.region_battery_checkboxes.value
            if not fleet_duids:
                self.region_info_pane.object = """
                <div style="background-color: #FFFCF0; border: 1px solid #B7B5AC; padding: 10px; border-radius: 5px;">
                    <p style="color: #AF3029;">No batteries selected. Check at least one battery.</p>
                </div>
                """
                self.region_chart_pane.object = None
                return

            actual_regions = selected_regions

            fleet_batteries = self.battery_info[
                self.battery_info['DUID'].isin(fleet_duids)
            ].copy()
            num_batteries = len(fleet_duids)
            num_total = len(self.region_battery_checkboxes.options)
            total_capacity_mw = fleet_batteries['Capacity(MW)'].sum()
            total_storage_mwh = fleet_batteries['Storage(MWh)'].sum()

            try:
                start_dt = pd.Timestamp(start_date)
                end_dt = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

                # Load generation data
                base_resolution = '5min' if frequency == '5m' else '30min'
                gen_data = load_generation_data(
                    start_date=start_dt,
                    end_date=end_dt,
                    resolution=base_resolution
                )

                if gen_data.empty:
                    self.region_info_pane.object = """
                    <div style="background-color: #FFFCF0; border: 1px solid #B7B5AC; padding: 10px; border-radius: 5px;">
                        <p style="color: #AF3029;">No generation data available for analysis period.</p>
                    </div>
                    """
                    self.region_chart_pane.object = None
                    return

                gen_data.columns = gen_data.columns.str.upper()

                # Filter for fleet DUIDs
                fleet_gen = gen_data[gen_data['DUID'].isin(fleet_duids)].copy()

                if fleet_gen.empty:
                    self.region_info_pane.object = """
                    <div style="background-color: #FFFCF0; border: 1px solid #B7B5AC; padding: 10px; border-radius: 5px;">
                        <p style="color: #AF3029;">No generation data found for batteries in this region.</p>
                    </div>
                    """
                    self.region_chart_pane.object = None
                    return

                # Aggregate across all batteries per timestamp
                fleet_gen['SETTLEMENTDATE'] = pd.to_datetime(fleet_gen['SETTLEMENTDATE'])
                agg_gen = fleet_gen.groupby('SETTLEMENTDATE').agg(
                    SCADAVALUE=('SCADAVALUE', 'sum')
                ).reset_index()

                # Load price data for all selected regions
                price_data = load_price_data(
                    start_date=start_dt,
                    end_date=end_dt,
                    resolution=base_resolution,
                    regions=actual_regions
                )
                if not price_data.empty:
                    if price_data.index.name == 'SETTLEMENTDATE':
                        price_data = price_data.reset_index()
                    price_data['SETTLEMENTDATE'] = pd.to_datetime(price_data['SETTLEMENTDATE'])
                    if len(actual_regions) > 1:
                        price_data = price_data.groupby('SETTLEMENTDATE').agg(
                            RRP=('RRP', 'mean')
                        ).reset_index()

                if price_data.empty:
                    self.region_info_pane.object = """
                    <div style="background-color: #FFFCF0; border: 1px solid #B7B5AC; padding: 10px; border-radius: 5px;">
                        <p style="color: #AF3029;">No price data available for analysis period.</p>
                    </div>
                    """
                    self.region_chart_pane.object = None
                    return

                # Merge generation and price
                analysis_data = agg_gen.merge(
                    price_data[['SETTLEMENTDATE', 'RRP']],
                    on='SETTLEMENTDATE',
                    how='outer'
                )
                analysis_data['SCADAVALUE'] = analysis_data['SCADAVALUE'].fillna(0)

                # Time multiplier
                if base_resolution == '5min':
                    base_time_multiplier = 1 / 12
                else:
                    base_time_multiplier = 0.5

                analysis_data['MWH'] = analysis_data['SCADAVALUE'] * base_time_multiplier
                analysis_data['REVENUE'] = analysis_data['MWH'] * analysis_data['RRP']

                # Aggregate by frequency
                resample_map = {'5m': None, '1h': '1h'}
                resample_freq = resample_map.get(frequency)
                if resample_freq is None:
                    aggregated_data = analysis_data.copy()
                else:
                    analysis_data['SETTLEMENTDATE'] = pd.to_datetime(analysis_data['SETTLEMENTDATE'])
                    aggregated_data = analysis_data.set_index('SETTLEMENTDATE').resample(resample_freq).agg({
                        'SCADAVALUE': 'mean',
                        'MWH': 'sum',
                        'REVENUE': 'sum',
                        'RRP': 'mean',
                    }).reset_index()

                # Split discharge / charge
                discharge_data = aggregated_data[aggregated_data['MWH'] > 0]
                charge_data = aggregated_data[aggregated_data['MWH'] < 0]

                total_discharge_mwh = discharge_data['MWH'].sum() if not discharge_data.empty else 0
                total_charge_mwh = abs(charge_data['MWH'].sum()) if not charge_data.empty else 0
                total_discharge_revenue = discharge_data['REVENUE'].sum() if not discharge_data.empty else 0
                total_charge_cost = abs(charge_data['REVENUE'].sum()) if not charge_data.empty else 0
                avg_discharge_price = total_discharge_revenue / total_discharge_mwh if total_discharge_mwh > 0 else 0
                avg_charge_price = total_charge_cost / total_charge_mwh if total_charge_mwh > 0 else 0
                avg_spread = avg_discharge_price - avg_charge_price
                total_spread = total_discharge_revenue - total_charge_cost

                total_days = (end_dt - start_dt).days + 1
                aggregated_data['SETTLEMENTDATE'] = pd.to_datetime(aggregated_data['SETTLEMENTDATE'])
                aggregated_data['date'] = aggregated_data['SETTLEMENTDATE'].dt.date
                days_discharged = aggregated_data[aggregated_data['SCADAVALUE'] > 0]['date'].nunique()
                days_charged = aggregated_data[aggregated_data['SCADAVALUE'] < 0]['date'].nunique()
                pct_days_discharged = (days_discharged / total_days * 100) if total_days > 0 else 0
                pct_days_charged = (days_charged / total_days * 100) if total_days > 0 else 0
                capacity_utilization = (total_discharge_mwh / (total_storage_mwh * total_days) * 100) if total_storage_mwh > 0 and total_days > 0 else 0

                # Format money strings
                def _fmt_money(val):
                    if abs(val) >= 1000000:
                        return f"${val/1000000:.2f}m"
                    return f"${val/1000:.1f}k"

                revenue_str = _fmt_money(total_discharge_revenue)
                cost_str = _fmt_money(total_charge_cost)
                total_spread_str = _fmt_money(total_spread)

                # --- Create plots ---
                from plotly.subplots import make_subplots
                import plotly.graph_objects as go

                plot_data = aggregated_data.set_index('SETTLEMENTDATE').sort_index()
                plot_data = plot_data[
                    (plot_data.index >= pd.Timestamp(start_date)) &
                    (plot_data.index <= pd.Timestamp(end_date) + pd.Timedelta(days=1))
                ]

                power_df = pd.DataFrame(index=plot_data.index)
                power_df['Discharge'] = plot_data['SCADAVALUE'].where(plot_data['SCADAVALUE'] > 0, 0)
                power_df['Charge'] = plot_data['SCADAVALUE'].where(plot_data['SCADAVALUE'] < 0, 0)

                # Dynamic label based on battery selection
                all_region_set = {'NSW1', 'QLD1', 'SA1', 'VIC1'}
                if set(selected_regions) == all_region_set:
                    region_str = 'NEM'
                else:
                    region_str = '+'.join(selected_regions)

                if num_batteries == 1:
                    batt = fleet_batteries.iloc[0]
                    region_label = f"{batt['Site Name']} ({batt['DUID']}) {batt['Capacity(MW)']:.0f} MW"
                elif num_batteries == num_total:
                    region_label = f"{region_str} Battery Fleet"
                else:
                    region_label = f"{region_str} ({num_batteries} of {num_total} batteries)"
                date_start_str = pd.Timestamp(start_date).strftime('%d %b %y')
                date_end_str = pd.Timestamp(end_date).strftime('%d %b %y')

                fig = make_subplots(
                    rows=2, cols=1, shared_xaxes=True,
                    vertical_spacing=0.08,
                    row_heights=[0.55, 0.45],
                )

                # Row 1 — Power (MW): step lines for discharge and charge
                fig.add_trace(go.Scatter(
                    x=power_df.index, y=power_df['Discharge'],
                    name='Discharge', mode='lines',
                    line=dict(color='#879A39', width=1, shape='hv'),
                    hovertemplate='%{y:.0f} MW<extra>Discharge</extra>',
                ), row=1, col=1)

                fig.add_trace(go.Scatter(
                    x=power_df.index, y=power_df['Charge'],
                    name='Charge', mode='lines',
                    line=dict(color='#AF3029', width=1, shape='hv'),
                    hovertemplate='%{y:.0f} MW<extra>Charge</extra>',
                ), row=1, col=1)

                fig.add_hline(y=0, line_width=0.5, line_color='#100F0F', opacity=0.8, row=1, col=1)

                # Row 2 — Price ($/MWh)
                use_log_scale = self.region_log_scale.value
                if use_log_scale:
                    ylabel_price = 'Price ($/MWh, symlog)'
                    price_label = 'Price (symlog)'
                    symlog_threshold = 300
                    price_values = plot_data['RRP'].copy()
                    pos_mask = price_values > symlog_threshold
                    neg_mask = price_values < -symlog_threshold
                    if pos_mask.any():
                        price_values.loc[pos_mask] = symlog_threshold * (1 + np.log10(price_values.loc[pos_mask] / symlog_threshold))
                    if neg_mask.any():
                        price_values.loc[neg_mask] = -symlog_threshold * (1 + np.log10(-price_values.loc[neg_mask] / symlog_threshold))
                else:
                    ylabel_price = 'Price ($/MWh)'
                    price_label = 'Price'
                    price_values = plot_data['RRP']

                fig.add_trace(go.Scatter(
                    x=plot_data.index, y=price_values,
                    name=price_label, mode='lines',
                    line=dict(color='#D0A215', width=2),
                    hovertemplate='$%{y:.0f}/MWh<extra>Price</extra>',
                ), row=2, col=1)

                fig.add_hline(y=0, line_width=0.5, line_color='#100F0F', opacity=0.8, row=2, col=1)

                # Layout
                fig.update_layout(
                    title=f"{region_label} — {date_start_str} – {date_end_str}",
                    paper_bgcolor=FLEXOKI_PAPER,
                    plot_bgcolor=FLEXOKI_PAPER,
                    height=600,
                    font=dict(color='#100F0F'),
                    legend=dict(
                        orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0,
                        bgcolor=FLEXOKI_PAPER, borderwidth=0,
                    ),
                    margin=dict(t=80, b=60),
                    annotations=[dict(
                        text='Data: AEMO, Plot, calcs: ITK',
                        xref='paper', yref='paper',
                        x=0, y=-0.08,
                        xanchor='left', yanchor='top',
                        showarrow=False,
                        font=dict(size=11, color='#6F6E69'),
                    )],
                )
                fig.update_xaxes(showgrid=False, row=1, col=1)
                fig.update_xaxes(title_text='Date', showgrid=False, row=2, col=1)
                fig.update_yaxes(title_text='Power (MW)', showgrid=False, row=1, col=1)
                fig.update_yaxes(title_text=ylabel_price, showgrid=False, row=2, col=1)

                self.region_chart_pane.object = fig

                # --- Create Time of Day chart ---
                self._create_region_tod_chart(analysis_data, region_label, start_date, end_date)

                # --- Create metrics HTML ---
                metrics_html = f"""
                <hr style="border-color: #B7B5AC; margin: 15px 0;">
                <table style="color: #100F0F; width: 100%; border-collapse: collapse;">
                    <tr><td colspan="2" style="color: #24837B; padding-bottom: 10px;"><strong>Performance Metrics:</strong></td></tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Average Discharge Price:</strong></td>
                        <td style="padding: 5px; text-align: right;">${avg_discharge_price:.0f}/MWh</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Average Charge Price:</strong></td>
                        <td style="padding: 5px; text-align: right;">${avg_charge_price:.0f}/MWh</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Average Spread:</strong></td>
                        <td style="padding: 5px; text-align: right; color: #24837B; font-weight: bold;">${avg_spread:.0f}/MWh</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Total Discharge Revenue:</strong></td>
                        <td style="padding: 5px; text-align: right; color: #24837B;">{revenue_str}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Total Charge Cost:</strong></td>
                        <td style="padding: 5px; text-align: right; color: #AF3029;">{cost_str}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Total Spread (Gross Profit):</strong></td>
                        <td style="padding: 5px; text-align: right; color: #BC5215; font-weight: bold;">{total_spread_str}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Total Discharge Energy:</strong></td>
                        <td style="padding: 5px; text-align: right;">{total_discharge_mwh:,.1f} MWh</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Total Charge Energy:</strong></td>
                        <td style="padding: 5px; text-align: right;">{total_charge_mwh:,.1f} MWh</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Days Discharged:</strong></td>
                        <td style="padding: 5px; text-align: right;">{pct_days_discharged:.1f}% ({days_discharged}/{total_days} days)</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Days Charged:</strong></td>
                        <td style="padding: 5px; text-align: right;">{pct_days_charged:.1f}% ({days_charged}/{total_days} days)</td>
                    </tr>
                    <tr>
                        <td style="padding: 5px;"><strong>Capacity Utilization:</strong></td>
                        <td style="padding: 5px; text-align: right; color: #BC5215;">{capacity_utilization:.1f}%</td>
                    </tr>
                </table>
                """

                # Adapt header for single battery vs fleet
                if num_batteries == 1:
                    batt = fleet_batteries.iloc[0]
                    duration = total_storage_mwh / total_capacity_mw if total_capacity_mw > 0 else 0
                    fleet_header = f"""
                    <h3 style="color: #24837B; margin-top: 0;">{batt['Site Name']}</h3>
                    <table style="color: #100F0F; width: 100%;">
                        <tr><td><strong>DUID:</strong></td><td>{batt['DUID']}</td></tr>
                        <tr><td><strong>Region:</strong></td><td>{batt['Region']}</td></tr>
                        <tr><td><strong>Power Capacity:</strong></td><td>{total_capacity_mw:.0f} MW</td></tr>
                        <tr><td><strong>Energy Storage:</strong></td><td>{total_storage_mwh:.0f} MWh</td></tr>
                        <tr><td><strong>Duration:</strong></td><td>{duration:.1f} hours</td></tr>
                    </table>"""
                else:
                    fleet_header = f"""
                    <h3 style="color: #24837B; margin-top: 0;">{region_label}</h3>
                    <table style="color: #100F0F; width: 100%;">
                        <tr><td><strong>Region:</strong></td><td>{region_str}</td></tr>
                        <tr><td><strong>Number of Batteries:</strong></td><td>{num_batteries}</td></tr>
                        <tr><td><strong>Total Capacity:</strong></td><td>{total_capacity_mw:.0f} MW</td></tr>
                        <tr><td><strong>Total Storage:</strong></td><td>{total_storage_mwh:.0f} MWh</td></tr>
                    </table>"""

                info_html = f"""
                <div style="background-color: #FFFCF0; border: 2px solid #24837B; padding: 15px; border-radius: 5px;">
                    {fleet_header}
                    <hr style="border-color: #B7B5AC; margin: 10px 0;">
                    <table style="color: #100F0F; width: 100%;">
                        <tr><td colspan="2" style="color: #24837B;"><strong>Analysis Parameters:</strong></td></tr>
                        <tr><td><strong>Date Range:</strong></td><td>{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}</td></tr>
                        <tr><td><strong>Frequency:</strong></td><td>{frequency}</td></tr>
                    </table>
                    {metrics_html}
                </div>
                """
                self.region_info_pane.object = info_html

                logger.info(f"Region analysis complete: {region_label}, {num_batteries} batteries, {total_capacity_mw:.0f} MW")

            except Exception as e:
                logger.error(f"Error analyzing region: {e}")
                import traceback
                traceback.print_exc()
                self.region_info_pane.object = f"""
                <div style="background-color: #FFFCF0; border: 1px solid #AF3029; padding: 10px; border-radius: 5px;">
                    <p style="color: #AF3029;">Error analyzing region: {str(e)}</p>
                </div>
                """
                self.region_chart_pane.object = None

        # Connect callbacks
        self.region_date_presets.param.watch(update_region_date_range, 'value')
        self.region_analyze_button.on_click(analyze_region)
        self.region_log_scale.param.watch(
            lambda event: analyze_region(event) if self.region_chart_pane.object is not None else None, 'value'
        )

        # --- Region Select All ---
        def on_region_select_all(event):
            if self._updating_regions:
                return
            self._updating_regions = True
            try:
                if event.new:
                    self.region_checkboxes.value = ['NSW1', 'QLD1', 'SA1', 'VIC1']
                else:
                    if len(self.region_checkboxes.value) == 4:
                        self.region_checkboxes.value = []
            finally:
                self._updating_regions = False
            # Cascade: rebuild owners and batteries
            self._update_owner_list()

        self.region_select_all.param.watch(on_region_select_all, 'value')

        # --- Region checkbox change → rebuild owners ---
        def on_region_change(event):
            if self._updating_regions:
                return
            self._updating_regions = True
            try:
                self.region_select_all.value = (len(event.new) == 4)
            finally:
                self._updating_regions = False
            self._update_owner_list()

        self.region_checkboxes.param.watch(on_region_change, 'value')

        # --- Owner Select All ---
        def on_owner_select_all(event):
            if self._updating_owners:
                return
            self._updating_owners = True
            try:
                if event.new:
                    self.owner_checkboxes.value = list(self.owner_checkboxes.options)
                else:
                    if len(self.owner_checkboxes.value) == len(self.owner_checkboxes.options):
                        self.owner_checkboxes.value = []
            finally:
                self._updating_owners = False
            # Cascade: rebuild batteries
            self._update_region_batteries()

        self.owner_select_all.param.watch(on_owner_select_all, 'value')

        # --- Owner checkbox change → rebuild batteries ---
        def on_owner_change(event):
            if self._updating_owners:
                return
            self._updating_owners = True
            try:
                self.owner_select_all.value = (
                    len(event.new) == len(self.owner_checkboxes.options)
                    and len(event.new) > 0
                )
            finally:
                self._updating_owners = False
            self._update_region_batteries()

        self.owner_checkboxes.param.watch(on_owner_change, 'value')

        # --- Battery Select All ---
        def on_battery_select_all(event):
            if self._updating_batteries:
                return
            self._updating_batteries = True
            try:
                if event.new:
                    all_duids = list(self.region_battery_checkboxes.options.values())
                    self.region_battery_checkboxes.value = all_duids
                else:
                    if len(self.region_battery_checkboxes.value) == len(self.region_battery_checkboxes.options):
                        self.region_battery_checkboxes.value = []
            finally:
                self._updating_batteries = False

        self.battery_select_all.param.watch(on_battery_select_all, 'value')

        # --- Individual battery toggle → update battery Select All ---
        def on_battery_toggle(event):
            if self._updating_batteries:
                return
            self._updating_batteries = True
            try:
                all_duids = list(self.region_battery_checkboxes.options.values())
                self.battery_select_all.value = (set(event.new) == set(all_duids))
            finally:
                self._updating_batteries = False

        self.region_battery_checkboxes.param.watch(on_battery_toggle, 'value')

        # Populate initial lists
        self._update_owner_list()

    def _update_owner_list(self):
        """Rebuild owner checkbox list based on selected regions, then cascade to batteries."""
        selected_regions = self.region_checkboxes.value
        fleet = self.battery_info[self.battery_info['Region'].isin(selected_regions)]
        owner_col = next((c for c in ('Owner', 'Participant') if c in fleet.columns), None)
        if owner_col and not fleet.empty:
            owners = sorted(fleet[owner_col].dropna().unique().tolist())
        else:
            owners = []
        self._updating_owners = True
        try:
            self.owner_checkboxes.options = owners
            self.owner_checkboxes.value = owners  # select all
            self.owner_select_all.value = True
        finally:
            self._updating_owners = False
        self._update_region_batteries()

    def _update_region_batteries(self):
        """Update battery checkbox list based on selected regions and owners."""
        selected_regions = self.region_checkboxes.value
        selected_owners = self.owner_checkboxes.value

        fleet = self.battery_info[self.battery_info['Region'].isin(selected_regions)].copy()

        # Filter by selected owners
        owner_col = next((c for c in ('Owner', 'Participant') if c in fleet.columns), None)
        if owner_col and selected_owners:
            fleet = fleet[fleet[owner_col].isin(selected_owners)]

        # Build checkbox options {display_label: duid}
        options = {}
        for _, b in fleet.sort_values('Capacity(MW)', ascending=False).iterrows():
            cap = b.get('Capacity(MW)', 0)
            label = f"{b['Site Name']} ({b['DUID']}) - {cap:.0f} MW"
            options[label] = b['DUID']

        self._updating_batteries = True
        try:
            self.region_battery_checkboxes.options = options
            self.region_battery_checkboxes.value = list(options.values())
            self.battery_select_all.value = True
        finally:
            self._updating_batteries = False

    def _create_region_tod_chart(self, analysis_data, region_label, start_date, end_date):
        """Create Time of Day Plotly chart from raw analysis_data (pre-aggregation)."""
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go

        try:
            tod = analysis_data.copy()
            tod['SETTLEMENTDATE'] = pd.to_datetime(tod['SETTLEMENTDATE'])
            tod['hour'] = tod['SETTLEMENTDATE'].dt.hour
            tod['discharge_mw'] = tod['SCADAVALUE'].where(tod['SCADAVALUE'] > 0, 0)
            tod['charge_mw'] = tod['SCADAVALUE'].where(tod['SCADAVALUE'] < 0, 0)

            tod_stats = tod.groupby('hour').agg(
                avg_discharge=('discharge_mw', 'mean'),
                avg_charge=('charge_mw', 'mean'),
                avg_price=('RRP', 'mean'),
            ).reset_index()

            fig = make_subplots(specs=[[{"secondary_y": True}]])

            fig.add_trace(go.Bar(
                x=tod_stats['hour'], y=tod_stats['avg_discharge'],
                name='Avg Discharge', marker_color='#879A39', opacity=0.85,
                hovertemplate='%{y:.0f} MW<extra>Discharge</extra>'
            ), secondary_y=False)

            fig.add_trace(go.Bar(
                x=tod_stats['hour'], y=tod_stats['avg_charge'],
                name='Avg Charge', marker_color='#AF3029', opacity=0.85,
                hovertemplate='%{y:.0f} MW<extra>Charge</extra>'
            ), secondary_y=False)

            fig.add_trace(go.Scatter(
                x=tod_stats['hour'], y=tod_stats['avg_price'],
                name='Avg Price', mode='lines+markers',
                line=dict(color='#D0A215', width=2.5),
                marker=dict(size=5, color='#D0A215'),
                hovertemplate='$%{y:.0f}/MWh<extra>Price</extra>'
            ), secondary_y=True)

            date_start_str = pd.Timestamp(start_date).strftime('%d %b %y')
            date_end_str = pd.Timestamp(end_date).strftime('%d %b %y')

            fig.update_layout(
                title=f"{region_label} — Time of Day {date_start_str} – {date_end_str}",
                paper_bgcolor=FLEXOKI_PAPER,
                plot_bgcolor=FLEXOKI_PAPER,
                height=520,
                barmode='relative',
                legend=dict(
                    orientation='h', yanchor='top', y=-0.18, xanchor='right', x=1,
                    bgcolor=FLEXOKI_PAPER, borderwidth=0,
                ),
                xaxis=dict(
                    title='Hour of Day', dtick=1, showgrid=False,
                    tickvals=list(range(0, 24)),
                    ticktext=[f'{h:02d}:00' for h in range(24)],
                ),
                margin=dict(t=60, b=100),
                font=dict(color='#100F0F'),
                annotations=[dict(
                    text='Data: AEMO, Plot, calcs: ITK',
                    xref='paper', yref='paper',
                    x=0, y=-0.22,
                    xanchor='left', yanchor='top',
                    showarrow=False,
                    font=dict(size=11, color='#6F6E69'),
                )],
            )
            fig.update_yaxes(title_text='Average Power (MW)', showgrid=False, secondary_y=False)
            fig.update_yaxes(
                title_text='Average Price ($/MWh)', showgrid=False,
                tickfont=dict(color='#D0A215'), title_font=dict(color='#D0A215'),
                secondary_y=True
            )

            self.region_tod_chart_pane.object = fig

            # --- TOD metrics panel ---
            peak_discharge_hour = int(tod_stats.loc[tod_stats['avg_discharge'].idxmax(), 'hour'])
            peak_discharge_mw = tod_stats['avg_discharge'].max()
            peak_charge_hour = int(tod_stats.loc[tod_stats['avg_charge'].idxmin(), 'hour'])
            peak_charge_mw = tod_stats['avg_charge'].min()
            highest_price_hour = int(tod_stats.loc[tod_stats['avg_price'].idxmax(), 'hour'])
            highest_price = tod_stats['avg_price'].max()
            lowest_price_hour = int(tod_stats.loc[tod_stats['avg_price'].idxmin(), 'hour'])
            lowest_price = tod_stats['avg_price'].min()

            bands = [
                ('Overnight', 0, 6),
                ('Morning', 6, 12),
                ('Afternoon', 12, 18),
                ('Evening', 18, 24),
            ]
            band_rows = ""
            for name, h_start, h_end in bands:
                band = tod_stats[(tod_stats['hour'] >= h_start) & (tod_stats['hour'] < h_end)]
                bd = band['avg_discharge'].mean()
                bc = band['avg_charge'].mean()
                bp = band['avg_price'].mean()
                band_rows += f"""
                <tr style="border-bottom: 1px solid #DAD8CE;">
                    <td style="padding: 4px;"><strong>{name}</strong> ({h_start:02d}–{h_end:02d})</td>
                    <td style="padding: 4px; text-align: right; color: #879A39;">{bd:.0f} MW</td>
                    <td style="padding: 4px; text-align: right; color: #AF3029;">{bc:.0f} MW</td>
                    <td style="padding: 4px; text-align: right; color: #D0A215;">${bp:.0f}</td>
                </tr>"""

            tod_html = f"""
            <div style="background-color: #FFFCF0; border: 2px solid #24837B; padding: 15px; border-radius: 5px;">
                <h3 style="color: #24837B; margin-top: 0;">Hourly Insights</h3>
                <table style="color: #100F0F; width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Peak Discharge Hour:</strong></td>
                        <td style="padding: 5px; text-align: right; color: #879A39;">{peak_discharge_hour:02d}:00 ({peak_discharge_mw:.0f} MW)</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Peak Charge Hour:</strong></td>
                        <td style="padding: 5px; text-align: right; color: #AF3029;">{peak_charge_hour:02d}:00 ({peak_charge_mw:.0f} MW)</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Highest Price Hour:</strong></td>
                        <td style="padding: 5px; text-align: right; color: #D0A215;">{highest_price_hour:02d}:00 (${highest_price:.0f}/MWh)</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #DAD8CE;">
                        <td style="padding: 5px;"><strong>Lowest Price Hour:</strong></td>
                        <td style="padding: 5px; text-align: right; color: #D0A215;">{lowest_price_hour:02d}:00 (${lowest_price:.0f}/MWh)</td>
                    </tr>
                </table>
                <hr style="border-color: #B7B5AC; margin: 12px 0;">
                <table style="color: #100F0F; width: 100%; border-collapse: collapse;">
                    <tr><td colspan="4" style="color: #24837B; padding-bottom: 8px;"><strong>Time Band Averages:</strong></td></tr>
                    <tr style="border-bottom: 1px solid #B7B5AC;">
                        <th style="padding: 4px; text-align: left;">Band</th>
                        <th style="padding: 4px; text-align: right;">Discharge</th>
                        <th style="padding: 4px; text-align: right;">Charge</th>
                        <th style="padding: 4px; text-align: right;">Price</th>
                    </tr>
                    {band_rows}
                </table>
            </div>
            """
            self.region_tod_info_pane.object = tod_html

        except Exception as e:
            logger.error(f"Error creating TOD chart: {e}")
            import traceback
            traceback.print_exc()
            self.region_tod_chart_pane.object = None
            self.region_tod_info_pane.object = f"""
            <div style="background-color: #FFFCF0; border: 1px solid #AF3029; padding: 10px; border-radius: 5px;">
                <p style="color: #AF3029;">Error creating Time of Day chart: {str(e)}</p>
            </div>
            """

    def _setup_callbacks(self):
        """Set up widget callbacks"""
        # Date preset callback — maps compact labels to original values
        def update_date_range(event):
            """Update date range based on preset selection"""
            compact = event.new
            preset = self._preset_map.get(compact, compact)
            current_end = self.end_date_picker.value

            if preset == '1 day':
                new_start = current_end - pd.Timedelta(days=1)
            elif preset == '7 days':
                new_start = current_end - pd.Timedelta(days=7)
            elif preset == '30 days':
                new_start = current_end - pd.Timedelta(days=30)
            elif preset == '90 days':
                new_start = current_end - pd.Timedelta(days=90)
            elif preset == '1 year':
                new_start = current_end - pd.Timedelta(days=365)
            else:  # All data
                new_start = pd.Timestamp('2020-01-01').date()

            self.start_date_picker.value = new_start

        # Update button callback
        def update_insights(event):
            """Update battery analysis based on current selections"""
            logger.info(f"Updating battery analysis for regions: {self.region_selector.value}")
            logger.info(f"Date range: {self.start_date_picker.value} to {self.end_date_picker.value}")

            # Clear the dynamic content pane
            self.dynamic_content_pane.object = ""

            # Battery analysis will be implemented here
            self.battery_content_pane.clear()
            self.battery_content_pane.append(pn.pane.HTML("""
            <div style="
                background-color: #FFFCF0;
                border: 2px solid #B7B5AC;
                border-radius: 10px;
                padding: 20px;
                margin: 10px;
                color: #100F0F;
            ">
                <h2 style="color: #24837B;">Battery Analysis Updated</h2>
                <p style="color: #100F0F;">Analysis for selected regions and date range will appear here.</p>
            </div>
            """))

        # Connect callbacks
        self.date_presets.param.watch(update_date_range, 'value')
        self.update_button.on_click(self._create_battery_lollipop_chart)
    
    def _calculate_battery_metrics(self, event=None):
        """Calculate metrics for all batteries in selected regions"""
        try:
            # Get selected region (now single selection)
            selected_region = self.region_selector.value
            if not selected_region:
                logger.warning("No region selected")
                return pd.DataFrame()
            
            # Handle NEM selection - aggregate all regions
            if selected_region == 'NEM':
                actual_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
            else:
                actual_regions = [selected_region]
            
            # Get date range
            start_date = pd.Timestamp(self.start_date_picker.value)
            end_date = pd.Timestamp(self.end_date_picker.value) + pd.Timedelta(hours=23, minutes=59)
            
            # Load generation and price data
            logger.info(f"Loading data for regions {actual_regions} from {start_date} to {end_date}")
            
            gen_data = load_generation_data(
                start_date=start_date,
                end_date=end_date,
                resolution='30min'
            )
            
            price_data = load_price_data(
                start_date=start_date,
                end_date=end_date,
                resolution='30min'
            )
            
            if gen_data.empty or price_data.empty:
                logger.warning("No data available for analysis")
                return pd.DataFrame()
            
            # Standardize column names
            gen_data.columns = gen_data.columns.str.upper()
            if price_data.index.name == 'SETTLEMENTDATE':
                price_data = price_data.reset_index()
            price_data.columns = price_data.columns.str.upper()
            
            # Filter for batteries in selected regions
            batteries_to_analyze = self.battery_info[
                self.battery_info['Region'].isin(actual_regions)
            ].copy()
            
            if batteries_to_analyze.empty:
                logger.warning("No batteries found in selected regions")
                return pd.DataFrame()
            
            # Calculate metrics for each battery
            metrics_list = []
            
            for _, battery in batteries_to_analyze.iterrows():
                duid = battery['DUID']
                
                # Filter generation data for this battery
                battery_gen = gen_data[gen_data['DUID'] == duid].copy()
                
                if battery_gen.empty:
                    continue
                
                # Merge with price data
                battery_gen = battery_gen.merge(
                    price_data[['SETTLEMENTDATE', 'REGIONID', 'RRP']],
                    on='SETTLEMENTDATE',
                    how='left'
                )
                
                # Filter for matching region
                battery_gen = battery_gen[battery_gen['REGIONID'] == battery['Region']]
                
                if battery_gen.empty:
                    continue
                
                # Separate charge and discharge
                discharge_data = battery_gen[battery_gen['SCADAVALUE'] > 0].copy()
                charge_data = battery_gen[battery_gen['SCADAVALUE'] < 0].copy()
                
                # Calculate metrics based on selected metric type
                metrics = {
                    'DUID': duid,
                    'Site Name': battery['Site Name'],
                    'Region': battery['Region'],
                    'Capacity_MW': battery.get('Capacity(MW)', 0),
                    'Storage_MWh': battery.get('Storage(MWh)', 0)
                }
                
                # Discharge metrics
                if not discharge_data.empty:
                    metrics['Discharge Energy'] = discharge_data['SCADAVALUE'].sum() / 2  # MWh for 30-min periods
                    metrics['Discharge Revenue'] = (discharge_data['SCADAVALUE'] * discharge_data['RRP'] / 2).sum()
                    metrics['Discharge Price'] = (
                        metrics['Discharge Revenue'] / metrics['Discharge Energy']
                        if metrics['Discharge Energy'] > 0 else 0
                    )
                else:
                    metrics['Discharge Price'] = 0
                    metrics['Discharge Energy'] = 0
                    metrics['Discharge Revenue'] = 0
                
                # Charge metrics
                if not charge_data.empty:
                    metrics['Charge Energy'] = abs(charge_data['SCADAVALUE'].sum()) / 2  # MWh
                    metrics['Charge Cost'] = abs((charge_data['SCADAVALUE'] * charge_data['RRP'] / 2).sum())
                    metrics['Charge Price'] = (
                        metrics['Charge Cost'] / metrics['Charge Energy']
                        if metrics['Charge Energy'] > 0 else 0
                    )
                else:
                    metrics['Charge Price'] = 0
                    metrics['Charge Energy'] = 0
                    metrics['Charge Cost'] = 0
                
                # Calculate spread
                metrics['Price Spread'] = metrics['Discharge Price'] - metrics['Charge Price']
                
                metrics_list.append(metrics)
            
            if not metrics_list:
                logger.warning("No battery data found for analysis")
                return pd.DataFrame()
            
            return pd.DataFrame(metrics_list)
            
        except Exception as e:
            logger.error(f"Error calculating battery metrics: {e}")
            return pd.DataFrame()
    
    def _create_battery_lollipop_chart(self, event=None):
        """Create lollipop chart for battery comparison using matplotlib"""
        try:
            logger.info("Creating battery lollipop chart")
            
            # Calculate metrics for all batteries
            metrics_df = self._calculate_battery_metrics()
            
            if metrics_df.empty:
                self.battery_info_pane.object = """
                <div style="background-color: #FFFCF0; border: 2px solid #AF3029; padding: 15px; border-radius: 10px;">
                    <p style="color: #AF3029;">No battery data available for the selected regions and date range.</p>
                </div>
                """
                self.battery_content_pane.clear()
                return
            
            # Get selected metric
            selected_metric = self.metric_selector.value
            
            # Sort by selected metric and get top 20
            metrics_df = metrics_df.sort_values(selected_metric, ascending=False).head(20)
            
            # Create display names (truncate if too long)
            metrics_df['Display Name'] = metrics_df.apply(
                lambda x: f"{x['Site Name'][:20]}..." if len(x['Site Name']) > 20 else x['Site Name'],
                axis=1
            )
            
            # Determine colors based on region when NEM is selected
            selected_region = self.region_selector.value
            if selected_region == 'NEM':
                # Use different colors for different regions (defined in __init__)
                colors = [self.region_colors.get(region, '#100F0F') for region in metrics_df['Region']]
            else:
                # Use metric-based colors for single region - Flexoki Light compatible
                if selected_metric in ['Discharge Revenue', 'Discharge Energy', 'Price Spread']:
                    base_color = '#879A39' if selected_metric != 'Price Spread' else None  # Flexoki Green
                    if selected_metric == 'Price Spread':
                        # Use green for positive, red for negative
                        colors = ['#879A39' if x > 0 else '#AF3029' for x in metrics_df[selected_metric]]
                    else:
                        colors = [base_color] * len(metrics_df)
                elif selected_metric in ['Charge Cost', 'Charge Energy']:
                    colors = ['#AF3029'] * len(metrics_df)  # Flexoki Red
                else:
                    colors = ['#3AA99F'] * len(metrics_df)  # Flexoki Cyan
            
            # Format values for display with zero decimal places
            if selected_metric in ['Discharge Revenue', 'Charge Cost']:
                # Format as currency with no decimals
                metrics_df['Formatted Value'] = metrics_df[selected_metric].apply(
                    lambda x: f"${x/1e6:.0f}M" if abs(x) >= 1e6 else f"${x/1e3:.0f}K"
                )
            elif selected_metric in ['Discharge Energy', 'Charge Energy']:
                # Format as MWh with no decimals
                metrics_df['Formatted Value'] = metrics_df[selected_metric].apply(
                    lambda x: f"{x:,.0f} MWh"
                )
            elif selected_metric in ['Discharge Price', 'Charge Price', 'Price Spread']:
                # Format as plain number (units in title)
                metrics_df['Formatted Value'] = metrics_df[selected_metric].apply(
                    lambda x: f"{x:.0f}"
                )
            else:
                metrics_df['Formatted Value'] = metrics_df[selected_metric].round(0).astype(int).astype(str)
            
            # Create lollipop chart using matplotlib - Flexoki Light theme
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches

            fig, ax = plt.subplots(figsize=(12, 6), facecolor=FLEXOKI_PAPER)
            ax.set_facecolor(FLEXOKI_PAPER)

            # X positions
            x_pos = range(len(metrics_df))
            y_values = metrics_df[selected_metric].values

            # Draw stems (vertical lines)
            for i, y in enumerate(y_values):
                ax.plot([i, i], [0, y], color=FLEXOKI_BORDER, linewidth=2, alpha=0.7)

            # Draw dots
            ax.scatter(x_pos, y_values, s=150, c=colors, zorder=5, edgecolors='#100F0F', linewidth=0.5)

            # Set x-axis labels
            ax.set_xticks(x_pos)
            ax.set_xticklabels(metrics_df['Display Name'].values, rotation=45, ha='right', color='#100F0F')

            # Set y-axis label and format
            ax.set_ylabel(selected_metric, color='#100F0F', fontsize=12)

            # Format y-axis values based on metric type
            if selected_metric in ['Discharge Revenue', 'Charge Cost']:
                # Format as currency
                ax.yaxis.set_major_formatter(plt.FuncFormatter(
                    lambda x, p: f'${x/1e6:.1f}M' if abs(x) >= 1e6 else f'${x/1e3:.0f}K'
                ))
            elif selected_metric in ['Discharge Energy', 'Charge Energy']:
                # Format as MWh
                ax.yaxis.set_major_formatter(plt.FuncFormatter(
                    lambda x, p: f'{x:,.0f}'
                ))
            elif selected_metric in ['Price Spread']:
                # Format as $/MWh, handle negative values
                ax.yaxis.set_major_formatter(plt.FuncFormatter(
                    lambda x, p: f'${x:.0f}'
                ))
            else:
                # Format as $/MWh
                ax.yaxis.set_major_formatter(plt.FuncFormatter(
                    lambda x, p: f'${x:.0f}'
                ))

            # Title - include units
            if selected_metric in ['Discharge Price', 'Charge Price', 'Price Spread']:
                title_text = f'Top 20 Batteries by {selected_metric} ($/MWh)'
            elif selected_metric in ['Discharge Revenue', 'Charge Cost']:
                title_text = f'Top 20 Batteries by {selected_metric} ($)'
            elif selected_metric in ['Discharge Energy', 'Charge Energy']:
                title_text = f'Top 20 Batteries by {selected_metric} (MWh)'
            else:
                title_text = f'Top 20 Batteries by {selected_metric}'
            ax.set_title(title_text, color='#100F0F', fontsize=14, pad=20)

            # No grid
            ax.grid(False)

            # Add horizontal line at y=0
            ax.axhline(y=0, color=FLEXOKI_BORDER, linewidth=1, alpha=0.5)

            # Tick colors
            ax.tick_params(colors='#100F0F')

            # Remove spines
            for spine in ax.spines.values():
                spine.set_visible(False)
            
            # Add value labels with alternating heights to avoid overlap
            for i, (val, fmt_val) in enumerate(zip(y_values, metrics_df['Formatted Value'].values)):
                # Alternate between two different vertical offsets for positive values
                if val >= 0:
                    # Even indices get normal position, odd indices get higher position
                    if i % 2 == 0:
                        offset = val * 0.06 if val > 0 else 8  # Offset above the dot
                    else:
                        offset = val * 0.12 if val > 0 else 18  # Larger offset for odd indices
                    ax.text(i, val + offset, fmt_val, ha='center', va='bottom', color='#100F0F',
                           fontsize=8, fontweight='bold')
                else:
                    # For negative values, place below
                    offset = val * 0.05 if val < 0 else -5
                    ax.text(i, val + offset, fmt_val, ha='center', va='top', color='#100F0F',
                           fontsize=8, fontweight='bold')

            # Add legend if NEM is selected to show region colors
            if selected_region == 'NEM':
                # Create custom legend entries for regions (excluding NEM itself)
                from matplotlib.patches import Patch
                legend_elements = []
                for region in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']:
                    if region in self.region_colors:
                        legend_elements.append(
                            Patch(facecolor=self.region_colors[region],
                                  edgecolor='#100F0F', label=region)
                        )

                # Add legend to the plot
                if legend_elements:
                    ax.legend(handles=legend_elements,
                             loc='upper right',
                             frameon=True,
                             fancybox=True,
                             shadow=False,
                             facecolor=FLEXOKI_PAPER,
                             edgecolor=FLEXOKI_BORDER,
                             labelcolor='#100F0F',
                             fontsize=9,
                             title='Regions',
                             title_fontsize=10)
                    # Style the legend title
                    legend = ax.get_legend()
                    if legend:
                        legend.get_title().set_color('#100F0F')

            # Add attribution at bottom right
            ax.text(0.99, 0.01, 'Data: AEMO, Plot: ITK',
                   transform=ax.transAxes, ha='right', va='bottom',
                   fontsize=8, color='#6F6E69', alpha=0.8)
            
            # Tight layout
            plt.tight_layout()
            
            # Convert matplotlib figure to Panel pane and update the container
            self.battery_content_pane.clear()
            self.battery_content_pane.append(pn.pane.Matplotlib(fig, tight=True, dpi=100))
            
            # Update info pane with summary statistics
            selected_regions_text = self.region_selector.value
            total_batteries = len(metrics_df)
            
            if selected_metric in ['Discharge Revenue', 'Charge Cost']:
                total_value = metrics_df[selected_metric].sum()
                total_text = f"${total_value/1e6:.2f}M" if abs(total_value) >= 1e6 else f"${total_value/1e3:.1f}K"
                avg_value = metrics_df[selected_metric].mean()
                avg_text = f"${avg_value/1e6:.2f}M" if abs(avg_value) >= 1e6 else f"${avg_value/1e3:.1f}K"
            else:
                total_value = metrics_df[selected_metric].sum()
                avg_value = metrics_df[selected_metric].mean()
                if selected_metric in ['Discharge Energy', 'Charge Energy']:
                    total_text = f"{total_value:,.0f} MWh"
                    avg_text = f"{avg_value:,.0f} MWh"
                else:
                    total_text = f"{total_value:.2f}"
                    avg_text = f"{avg_value:.2f}"
            
            info_html = f"""
            <div style="background-color: #FFFCF0; border: 2px solid #24837B; padding: 15px; border-radius: 10px;">
                <h3 style="color: #24837B; margin-top: 0;">Battery Analysis Summary</h3>
                <table style="color: #100F0F; width: 100%;">
                    <tr><td><strong>Regions:</strong></td><td>{selected_regions_text}</td></tr>
                    <tr><td><strong>Date Range:</strong></td><td>{self.start_date_picker.value} to {self.end_date_picker.value}</td></tr>
                    <tr><td><strong>Metric:</strong></td><td>{selected_metric}</td></tr>
                    <tr><td><strong>Batteries Shown:</strong></td><td>{total_batteries} (top performers)</td></tr>
                    <tr><td><strong>Total {selected_metric}:</strong></td><td>{total_text}</td></tr>
                    <tr><td><strong>Average {selected_metric}:</strong></td><td>{avg_text}</td></tr>
                </table>
            </div>
            """
            self.battery_info_pane.object = info_html

            logger.info(f"Lollipop chart created for {total_batteries} batteries")

        except Exception as e:
            logger.error(f"Error creating lollipop chart: {e}")
            self.battery_info_pane.object = f"""
            <div style="background-color: #FFFCF0; border: 2px solid #AF3029; padding: 15px; border-radius: 10px;">
                <p style="color: #AF3029;">Error creating chart: {str(e)}</p>
            </div>
            """
            self.battery_content_pane.clear()
    
    def _setup_volatility_controls(self):
        """Set up volatility chart specific controls"""
        # Volatility chart pane
        self.volatility_chart_pane = pn.pane.Plotly(
            object=None,
            sizing_mode='stretch_width',
            height=500
        )
        
        # Don't connect callbacks here - chart should only update on button click
        # Removed automatic watches on region_selector, volatility_window_selector, and log_scale_checkbox
        
        # Delay initial chart creation to avoid hanging on tab load
        # Chart will be created when user clicks update button
        self._show_placeholder_message()
    
    def _show_placeholder_message(self):
        """Show placeholder message for volatility chart"""
        fig = go.Figure()
        fig.add_annotation(
            text='Select a region and smoothing window to generate the volatility chart',
            xref='paper', yref='paper', x=0.5, y=0.5,
            showarrow=False, font=dict(size=14, color='#100F0F'),
        )
        fig.update_layout(
            autosize=True, height=500,
            paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
            xaxis=dict(visible=False), yaxis=dict(visible=False),
        )
        self.volatility_chart_pane.object = fig
    
    def _calculate_rolling_statistics(self, df: pd.DataFrame, window_days: int) -> pd.DataFrame:
        """Calculate rolling mean and standard deviation for volatility bands"""
        # Convert to numeric window (30-minute periods per day = 48)
        window = window_days * 48
        
        # Calculate rolling statistics
        df['rolling_mean'] = df['rrp'].rolling(window=window, center=True).mean()
        df['rolling_std'] = df['rrp'].rolling(window=window, center=True).std()
        
        # Calculate bands
        df['upper_1std'] = df['rolling_mean'] + df['rolling_std']
        df['lower_1std'] = df['rolling_mean'] - df['rolling_std']
        df['upper_2std'] = df['rolling_mean'] + 2 * df['rolling_std']
        df['lower_2std'] = df['rolling_mean'] - 2 * df['rolling_std']
        
        return df
    
    def _apply_loess_with_bands(self, df: pd.DataFrame, window_days: int) -> pd.DataFrame:
        """Apply LOESS smoothing and calculate volatility bands"""
        if not HAS_LOESS:
            logger.warning("LOESS not available, falling back to rolling statistics")
            return self._calculate_rolling_statistics(df, window_days)
        
        # Since the data is already 30-minute, we can work with it directly
        # For better performance with 5 years of data, downsample to daily for LOESS
        logger.info(f"Applying LOESS to {len(df)} 30-minute data points")
        
        # Downsample to daily for LOESS calculation (48x reduction)
        df_daily = df.set_index('settlementdate').resample('1D').agg({
            'rrp': 'mean',
            'regionid': 'first'
        }).reset_index()
        
        logger.info(f"Downsampled to {len(df_daily)} daily points for LOESS")
        
        # Convert datetime to numeric for LOESS
        date_numeric = pd.to_datetime(df_daily['settlementdate']).astype(np.int64) / 1e9
        
        # Calculate fraction based on window size
        # For better trend capture, use a larger fraction
        total_days = (df_daily['settlementdate'].max() - df_daily['settlementdate'].min()).days
        # Make fraction larger to capture trends better - at least 0.05
        frac = min(0.5, max(0.05, window_days * 2 / total_days))
        
        logger.info(f"Applying LOESS with frac={frac:.4f} on {len(df_daily)} daily points for {window_days}-day smoothing")
        
        # Apply LOESS to price
        smoothed_price = lowess(
            df_daily['rrp'].values,
            date_numeric,
            frac=frac,
            it=0,
            return_sorted=False
        )
        
        df_daily['smoothed_price'] = smoothed_price
        
        # Calculate residuals on original 30-minute data for accurate volatility
        # Interpolate smoothed daily prices back to 30-minute resolution
        df = df.sort_values('settlementdate')
        df['smoothed_price'] = np.interp(
            pd.to_datetime(df['settlementdate']).astype(np.int64),
            pd.to_datetime(df_daily['settlementdate']).astype(np.int64),
            df_daily['smoothed_price'].values
        )
        
        # Calculate residuals
        df['residual'] = df['rrp'] - df['smoothed_price']
        
        # Calculate rolling standard deviation of residuals on 30-minute data
        window = window_days * 48  # 48 half-hour periods per day
        df['rolling_std'] = df['residual'].rolling(window=window, center=True).std()
        
        # Smooth the standard deviation using daily aggregation
        df_daily_std = df.set_index('settlementdate').resample('1D').agg({
            'rolling_std': 'mean'
        }).reset_index()
        
        # Apply LOESS to the rolling std
        smoothed_std = lowess(
            df_daily_std['rolling_std'].ffill().bfill().values,
            pd.to_datetime(df_daily_std['settlementdate']).astype(np.int64) / 1e9,
            frac=frac,
            it=0,
            return_sorted=False
        )
        
        # Interpolate smoothed std back to 30-minute resolution
        df['smoothed_std'] = np.interp(
            pd.to_datetime(df['settlementdate']).astype(np.int64),
            pd.to_datetime(df_daily_std['settlementdate']).astype(np.int64),
            smoothed_std
        )
        
        # Calculate bands
        df['upper_1std'] = df['smoothed_price'] + df['smoothed_std']
        df['lower_1std'] = df['smoothed_price'] - df['smoothed_std']
        df['upper_2std'] = df['smoothed_price'] + 2 * df['smoothed_std']
        df['lower_2std'] = df['smoothed_price'] - 2 * df['smoothed_std']
        
        logger.info("LOESS calculation complete")
        return df
    
    def _update_volatility_chart(self, event=None):
        """Update the volatility analysis chart"""
        try:
            # Get all selected regions
            if not self.region_selector.value:
                logger.warning("No region selected")
                self._show_placeholder_message()
                return
            
            selected_regions = self.region_selector.value  # Use all selected regions
            logger.info(f"Starting volatility chart creation for regions: {selected_regions}, window: {self.volatility_window_selector.value}")
            
            # Load all available price data (5 years)
            start_date = datetime(2020, 1, 1)
            end_date = datetime.now()
            logger.info(f"Loading price data from {start_date} to {end_date}")
            
            # Load price data using the function
            price_data = load_price_data(
                start_date=start_date,
                end_date=end_date,
                resolution='30min'
            )
            
            if price_data.empty:
                logger.warning("No price data available")
                self.volatility_chart_pane.object = None
                return
            
            # Reset index if SETTLEMENTDATE is the index
            if price_data.index.name == 'SETTLEMENTDATE':
                price_data = price_data.reset_index()
            
            # Check column names
            if 'REGIONID' in price_data.columns:
                region_col = 'REGIONID'
                price_col = 'RRP'
                date_col = 'SETTLEMENTDATE'
            else:
                region_col = 'regionid'
                price_col = 'rrp'
                date_col = 'settlementdate'
            
            # Extract window days from selector
            window_map = {
                '7 days': 7,
                '30 days': 30,
                '90 days': 90,
                '180 days': 180
            }
            window_days = window_map[self.volatility_window_selector.value]
            
            # Process each selected region and collect plots
            all_plots = []
            region_colors = {
                'NSW1': '#879A39',   # Flexoki Green
                'QLD1': '#BC5215',   # Flexoki Orange
                'SA1': '#CE5D97',    # Flexoki Magenta
                'TAS1': '#3AA99F',   # Flexoki Cyan
                'VIC1': '#8B7EC8',   # Flexoki Purple
            }
            # RGBA helpers for fill bands
            def _hex_to_rgba(hex_color, alpha):
                h = hex_color.lstrip('#')
                r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
                return f'rgba({r},{g},{b},{alpha})'
            
            mean_prices = {}  # Store mean prices for each region
            
            for idx, region in enumerate(selected_regions):
                # Filter data for this region
                region_data = price_data[price_data[region_col] == region].copy()
                logger.info(f"Filtered to {len(region_data)} records for region {region}")
                
                # Standardize column names
                region_data = region_data.rename(columns={
                    date_col: 'settlementdate',
                    region_col: 'regionid',
                    price_col: 'rrp'
                })
                
                if region_data.empty:
                    logger.warning(f"No data for region {region}")
                    continue
                
                # Sort by date
                region_data = region_data.sort_values('settlementdate')
                
                # Apply LOESS smoothing with bands
                logger.info(f"Applying smoothing for {region} with window={window_days} days")
                if HAS_LOESS:
                    region_data = self._apply_loess_with_bands(region_data, window_days)
                    price_col_to_use = 'smoothed_price'
                else:
                    region_data = self._calculate_rolling_statistics(region_data, window_days)
                    price_col_to_use = 'rolling_mean'
                
                # Drop rows with NaN values for clean plotting
                plot_data = region_data.dropna(subset=[price_col_to_use, 'upper_2std', 'lower_2std'])
                
                if plot_data.empty:
                    logger.warning(f"No data after processing for {region}")
                    continue
                
                # Calculate and store mean price
                mean_prices[region] = region_data['rrp'].mean()
                
                # Get color for this region
                color = region_colors.get(region, 'white')
                
                # Store plot data for this region
                all_plots.append({
                    'region': region,
                    'color': color,
                    'dates': plot_data['settlementdate'],
                    'price': plot_data[price_col_to_use],
                    'upper_2std': plot_data['upper_2std'],
                    'lower_2std': plot_data['lower_2std'],
                    'upper_1std': plot_data['upper_1std'],
                    'lower_1std': plot_data['lower_1std'],
                })

            if not all_plots:
                logger.warning("No plots created")
                self.volatility_chart_pane.object = None
                return

            # Build Plotly figure
            fig = go.Figure()

            for p in all_plots:
                # 2 std band (lighter shade)
                fig.add_trace(go.Scatter(
                    x=p['dates'], y=p['upper_2std'],
                    mode='lines', line=dict(width=0),
                    showlegend=False, hoverinfo='skip',
                ))
                fig.add_trace(go.Scatter(
                    x=p['dates'], y=p['lower_2std'],
                    mode='lines', line=dict(width=0),
                    fill='tonexty', fillcolor=_hex_to_rgba(p['color'], 0.1),
                    name=f"{p['region']} ±2σ",
                    showlegend=True, hoverinfo='skip',
                ))
                # 1 std band (darker shade)
                fig.add_trace(go.Scatter(
                    x=p['dates'], y=p['upper_1std'],
                    mode='lines', line=dict(width=0),
                    showlegend=False, hoverinfo='skip',
                ))
                fig.add_trace(go.Scatter(
                    x=p['dates'], y=p['lower_1std'],
                    mode='lines', line=dict(width=0),
                    fill='tonexty', fillcolor=_hex_to_rgba(p['color'], 0.2),
                    name=f"{p['region']} ±1σ",
                    showlegend=True, hoverinfo='skip',
                ))
                # Price line
                fig.add_trace(go.Scatter(
                    x=p['dates'], y=p['price'],
                    mode='lines', line=dict(color=p['color'], width=2),
                    name=p['region'],
                ))

            # Create title with mean prices
            title_parts = [f'Price Volatility Analysis ({self.volatility_window_selector.value} smoothing)']
            if mean_prices:
                avg_text = ' - '.join([f'{r}: ${p:.0f}' for r, p in mean_prices.items()])
                title_parts.append(f'Avg: {avg_text}')
            title = ' - '.join(title_parts)

            use_log = self.log_scale_checkbox.value
            if use_log:
                title = title + ' - Symlog Scale'
                ylabel = 'Price ($/MWh) - Symlog Scale'
            else:
                ylabel = 'Price ($/MWh)'

            fig.update_layout(
                title=title,
                autosize=True, height=500,
                paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
                xaxis=dict(title='Date', showgrid=False),
                yaxis=dict(title=ylabel, showgrid=False, type='log' if use_log else 'linear', tickformat='$.0f'),
                legend=dict(bgcolor=FLEXOKI_PAPER, borderwidth=0),
                font=dict(color='#100F0F'),
            )

            self.volatility_chart_pane.object = fig
            logger.info(f"Volatility chart created successfully for regions: {selected_regions}")
            
        except Exception as e:
            logger.error(f"Error creating volatility chart: {e}")
            import traceback
            traceback.print_exc()
            self.volatility_chart_pane.object = None
    
    def _generate_initial_table(self):
        """Generate comparison table for all regions on initial load"""
        all_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        self._create_comparison_table(all_regions)
    
    def _update_comparison_table(self):
        """Create comparison table for selected regions"""
        try:
            if not self.region_selector.value:
                self.comparison_table_pane.object = "<p>No regions selected</p>"
                return
            
            selected_regions = self.region_selector.value
            self._create_comparison_table(selected_regions)
        except Exception as e:
            logger.error(f"Error updating comparison table: {e}")
            import traceback
            traceback.print_exc()
    
    def _create_comparison_table(self, regions):
        """Create comparison table for specified regions"""
        try:
            logger.info(f"Creating comparison table for regions: {regions}")
            
            # Define time periods
            first_year_start = datetime(2020, 1, 1)
            first_year_end = datetime(2020, 12, 31, 23, 59, 59)
            
            yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
            last_year_start = yesterday - timedelta(days=365)
            last_year_end = yesterday
            
            # Create table data
            table_data = []
            
            for region in regions:
                # Get price data for 2020
                price_2020 = load_price_data(
                    start_date=first_year_start,
                    end_date=first_year_end,
                    regions=[region],
                    resolution='30min'
                )
                
                # Get price data for last 12 months
                price_last12 = load_price_data(
                    start_date=last_year_start,
                    end_date=last_year_end,
                    regions=[region],
                    resolution='30min'
                )
                
                # Calculate price statistics
                if not price_2020.empty:
                    if price_2020.index.name == 'SETTLEMENTDATE':
                        price_2020 = price_2020.reset_index()
                    price_col = 'RRP' if 'RRP' in price_2020.columns else 'rrp'
                    avg_2020 = price_2020[price_col].mean()
                    std_2020 = price_2020[price_col].std()
                else:
                    avg_2020 = 0
                    std_2020 = 0
                
                if not price_last12.empty:
                    if price_last12.index.name == 'SETTLEMENTDATE':
                        price_last12 = price_last12.reset_index()
                    price_col = 'RRP' if 'RRP' in price_last12.columns else 'rrp'
                    avg_last12 = price_last12[price_col].mean()
                    std_last12 = price_last12[price_col].std()
                else:
                    avg_last12 = 0
                    std_last12 = 0
                
                # Get generation data for VRE calculation using query manager
                logger.info(f"Loading generation data for {region} - 2020")
                gen_2020 = self.query_manager.query_generation_by_fuel(
                    start_date=first_year_start,
                    end_date=first_year_end,
                    region=region,
                    resolution='30min'
                )
                
                logger.info(f"Loading generation data for {region} - last 12 months")
                gen_last12 = self.query_manager.query_generation_by_fuel(
                    start_date=last_year_start,
                    end_date=last_year_end,
                    region=region,
                    resolution='30min'
                )
                
                # Also load rooftop solar data
                from aemo_dashboard.shared.adapter_selector import load_rooftop_data
                
                rooftop_2020 = load_rooftop_data(
                    start_date=first_year_start,
                    end_date=first_year_end
                )
                
                rooftop_last12 = load_rooftop_data(
                    start_date=last_year_start,
                    end_date=last_year_end
                )
                
                # Calculate VRE share (including rooftop)
                vre_share_2020 = self._calculate_vre_share(gen_2020, region, rooftop_2020)
                vre_share_last12 = self._calculate_vre_share(gen_last12, region, rooftop_last12)
                
                # Calculate coefficient of variation (CV)
                cv_2020 = (std_2020 / avg_2020 * 100) if avg_2020 > 0 else 0
                cv_last12 = (std_last12 / avg_last12 * 100) if avg_last12 > 0 else 0
                
                # Add row data with no decimal places
                table_data.append({
                    'Region': region,
                    'Period': '2020',
                    'Avg Price ($/MWh)': f"${avg_2020:.0f}",
                    'Variability* (%)': f"{cv_2020:.0f}%",
                    'VRE Share (%)': f"{vre_share_2020:.0f}%"
                })
                
                table_data.append({
                    'Region': region,
                    'Period': f'Last 12mo',
                    'Avg Price ($/MWh)': f"${avg_last12:.0f}",
                    'Variability* (%)': f"{cv_last12:.0f}%",
                    'VRE Share (%)': f"{vre_share_last12:.0f}%"
                })
            
            # Create HTML table
            if table_data:
                df_table = pd.DataFrame(table_data)
                
                # Calculate table width based on number of regions
                table_width = 250 + (len(regions) * 140)  # Base width + width per region
                
                # Pivot to get regions as column groups
                html = '<div style="margin: 20px 0;">'
                html += '<h3 style="color: #24837B;">Regional Comparison: 2020 vs Last 12 Months</h3>'
                html += f'<table style="border-collapse: collapse; width: {table_width}px; font-size: 13px; color: #100F0F;">'

                # Header row
                html += '<thead><tr style="background-color: #E6E4D9;">'
                html += '<th style="border: 1px solid #B7B5AC; padding: 5px 8px; text-align: left;">Metric</th>'
                for region in regions:
                    html += f'<th colspan="2" style="border: 1px solid #B7B5AC; padding: 5px 8px; text-align: center;">{region}</th>'
                html += '</tr>'

                # Sub-header row
                html += '<tr style="background-color: #F2F0E5;">'
                html += '<th style="border: 1px solid #B7B5AC; padding: 5px 8px;"></th>'
                for region in regions:
                    html += '<th style="border: 1px solid #B7B5AC; padding: 5px 8px; text-align: center; font-size: 12px;">2020</th>'
                    html += '<th style="border: 1px solid #B7B5AC; padding: 5px 8px; text-align: center; font-size: 12px;">Last 12mo</th>'
                html += '</tr></thead>'

                # Data rows
                html += '<tbody>'
                metrics = ['Avg Price ($/MWh)', 'Variability* (%)', 'VRE Share (%)']

                for metric in metrics:
                    html += '<tr style="background-color: #FFFCF0;">'
                    html += f'<td style="border: 1px solid #B7B5AC; padding: 5px 8px; font-weight: bold; font-size: 12px;">{metric}</td>'

                    for region in regions:
                        # Get 2020 value
                        val_2020 = df_table[(df_table['Region'] == region) & (df_table['Period'] == '2020')][metric].values[0]
                        val_last12 = df_table[(df_table['Region'] == region) & (df_table['Period'] == 'Last 12mo')][metric].values[0]

                        html += f'<td style="border: 1px solid #B7B5AC; padding: 5px 8px; text-align: right; font-size: 12px;">{val_2020}</td>'
                        html += f'<td style="border: 1px solid #B7B5AC; padding: 5px 8px; text-align: right; font-size: 12px;">{val_last12}</td>'

                    html += '</tr>'

                html += '</tbody></table>'
                html += '<p style="font-size: 11px; color: #6F6E69; margin-top: 5px; font-style: italic;">* Variability = Coefficient of Variation (CV) = Standard Deviation / Mean x 100%</p>'
                html += '</div>'
                
                self.comparison_table_pane.object = html
                logger.info("Comparison table created successfully")
            else:
                self.comparison_table_pane.object = "<p>No data available for comparison</p>"
                
        except Exception as e:
            logger.error(f"Error creating comparison table: {e}")
            import traceback
            traceback.print_exc()
            self.comparison_table_pane.object = f"<p>Error creating table: {str(e)}</p>"
    
    def _calculate_vre_share(self, gen_data: pd.DataFrame, region: str, rooftop_data: pd.DataFrame = None) -> float:
        """Calculate VRE share for a specific region including rooftop solar"""
        try:
            # If gen_data already has fuel_type aggregations (from query_generation_by_fuel)
            if not gen_data.empty and 'fuel_type' in gen_data.columns:
                # Data is already aggregated by fuel type
                # Group by fuel type and calculate average generation
                fuel_averages = gen_data.groupby('fuel_type')['total_generation_mw'].mean()
                
                # Calculate total generation average
                total_gen_mw = fuel_averages.sum()
                
                # Calculate VRE average (Wind + Solar)
                vre_fuels = ['Wind', 'Solar']
                vre_mw = fuel_averages[fuel_averages.index.isin(vre_fuels)].sum()
                
                logger.info(f"Utility-scale generation for {region}: Total avg: {total_gen_mw:.1f} MW, VRE avg: {vre_mw:.1f} MW")
                
                # Add rooftop if available
                if rooftop_data is not None and not rooftop_data.empty:
                    if region == 'NEM':
                        # For NEM, sum all regions
                        region_cols = [col for col in rooftop_data.columns if col != 'settlementdate']
                        rooftop_avg = rooftop_data[region_cols].sum(axis=1).mean()
                    elif region in rooftop_data.columns:
                        rooftop_avg = rooftop_data[region].mean()
                    else:
                        rooftop_avg = 0.0
                    
                    # Add rooftop to totals
                    vre_mw += rooftop_avg
                    total_gen_mw += rooftop_avg
                    logger.info(f"Added rooftop solar: {rooftop_avg:.1f} MW average")
                
                # Calculate VRE share
                if total_gen_mw > 0:
                    vre_share = (vre_mw / total_gen_mw) * 100
                    logger.info(f"VRE share for {region}: {vre_share:.1f}% (VRE: {vre_mw:.0f} MW, Total: {total_gen_mw:.0f} MW)")
                    return vre_share
                else:
                    return 0.0
            
            # Fallback to old method if data is not aggregated by fuel type
            # Use a simpler approach - calculate average MW for the period
            total_vre_mw = 0.0
            total_gen_mw = 0.0
            
            # First handle utility-scale generation
            if not gen_data.empty:
                # Load DUID mapping to get region and fuel type information
                import pickle
                gen_info_path = self.config.gen_info_file
                
                if not gen_info_path.exists():
                    logger.warning(f"Gen info file not found: {gen_info_path}")
                    # Try without region filtering - just use fuel types
                    if 'fuel_type' in gen_data.columns or 'FUEL_TYPE' in gen_data.columns:
                        fuel_col = 'fuel_type' if 'fuel_type' in gen_data.columns else 'FUEL_TYPE'
                        value_col = 'scadavalue' if 'scadavalue' in gen_data.columns else 'SCADAVALUE'
                        
                        # Calculate averages for all generation
                        total_gen_mw = gen_data[value_col].mean()
                        
                        # Calculate VRE average
                        vre_fuels = ['Wind', 'Solar', 'Solar Utility', 'Wind Onshore']
                        vre_data = gen_data[gen_data[fuel_col].isin(vre_fuels)]
                        if not vre_data.empty:
                            total_vre_mw = vre_data[value_col].mean()
                        
                        logger.info(f"Using fuel type data without region filter - VRE: {total_vre_mw:.0f} MW, Total: {total_gen_mw:.0f} MW")
                else:
                    with open(gen_info_path, 'rb') as f:
                        gen_info = pickle.load(f)
                    
                    # Create DUID to region and fuel type mapping
                    duid_info = {}
                    for _, row in gen_info.iterrows():
                        duid = row.get('DUID', row.get('duid', ''))
                        if duid:
                            duid_info[duid] = {
                                'region': row.get('REGIONID', row.get('regionid', row.get('Region ID', ''))),
                                'fuel': row.get('Fuel Source - Descriptor', row.get('fuel_type', row.get('Fuel Source', '')))
                            }
                    
                    # Add region and fuel type to generation data
                    if 'duid' in gen_data.columns:
                        duid_col = 'duid'
                    elif 'DUID' in gen_data.columns:
                        duid_col = 'DUID'
                    else:
                        logger.warning("No DUID column found in generation data")
                        return 0.0
                    
                    gen_data['region'] = gen_data[duid_col].map(lambda x: duid_info.get(x, {}).get('region', ''))
                    gen_data['fuel_type'] = gen_data[duid_col].map(lambda x: duid_info.get(x, {}).get('fuel', ''))
                    
                    # Filter for region
                    region_data = gen_data[gen_data['region'] == region].copy()
                    
                    if not region_data.empty:
                        # Get value column
                        value_col = 'scadavalue' if 'scadavalue' in region_data.columns else 'SCADAVALUE'
                        
                        # Calculate average MW for ALL fuel types
                        total_gen_mw = region_data[value_col].mean()
                        
                        # Calculate average MW for VRE only
                        vre_fuels = ['Wind', 'Solar', 'Solar Utility', 'Wind Onshore']
                        vre_data = region_data[region_data['fuel_type'].isin(vre_fuels)]
                        if not vre_data.empty:
                            total_vre_mw = vre_data[value_col].mean()
                        
                        logger.info(f"Utility generation for {region} - VRE avg: {total_vre_mw:.0f} MW, Total avg: {total_gen_mw:.0f} MW")
            
            # Add rooftop solar if available
            if rooftop_data is not None and not rooftop_data.empty:
                # Rooftop data is in wide format with regions as columns
                if region in rooftop_data.columns:
                    # Calculate average rooftop generation for this region
                    rooftop_avg_mw = rooftop_data[region].mean()
                    
                    if not pd.isna(rooftop_avg_mw) and rooftop_avg_mw > 0:
                        # Add rooftop to both VRE and total
                        total_vre_mw += rooftop_avg_mw
                        total_gen_mw += rooftop_avg_mw
                        logger.info(f"Rooftop solar for {region}: avg {rooftop_avg_mw:.0f} MW")
                    else:
                        logger.info(f"No rooftop solar data for {region}")
                else:
                    logger.warning(f"Region {region} not found in rooftop data columns: {list(rooftop_data.columns)}")
            
            # Calculate final VRE share
            if total_gen_mw > 0:
                vre_share = (total_vre_mw / total_gen_mw) * 100
                logger.info(f"Total VRE share for {region}: {vre_share:.1f}% (VRE avg: {total_vre_mw:.0f} MW, Total avg: {total_gen_mw:.0f} MW)")
                return vre_share
            else:
                logger.warning(f"No generation found for {region}")
                return 0.0
                
        except Exception as e:
            logger.error(f"Error calculating VRE share: {e}")
            import traceback
            traceback.print_exc()
            return 0.0
        
    def create_layout(self) -> pn.Column:
        """Create the complete batteries tab layout with subtabs"""

        # Create Overview subtab content — horizontal control bar + full-width charts

        # Row 1: update button + region selector (inline) + frequency toggle
        controls_row1 = pn.Row(
            self.update_button,
            pn.Spacer(width=20),
            pn.pane.Markdown("**Region**", margin=(8, 6, 0, 0)),
            self.region_selector,
            pn.Spacer(width=20),
            pn.pane.Markdown("**Freq**", margin=(8, 6, 0, 0)),
            self.aggregate_selector,
            align='center'
        )

        # Row 2: date presets + date pickers + metric selector
        controls_row2 = pn.Row(
            self.date_presets,
            pn.Spacer(width=15),
            self.start_date_picker,
            pn.Spacer(width=8),
            self.end_date_picker,
            pn.Spacer(width=20),
            self.metric_selector,
            align='center'
        )

        controls_bar = pn.Column(
            controls_row1,
            controls_row2,
            sizing_mode='stretch_width',
            styles={
                'border-bottom': '1px solid #CECDC3',
                'padding-bottom': '8px',
                'margin-bottom': '10px'
            }
        )

        # Main content area - battery analysis with lollipop chart
        main_content = pn.Column(
            self.battery_info_pane,
            pn.Spacer(height=10),
            self.battery_content_pane,
            pn.Spacer(height=20),
            self.dynamic_content_pane,
            sizing_mode='stretch_width'
        )

        # Full-width overview: control bar on top, charts below
        overview_tab = pn.Column(
            controls_bar,
            main_content,
            sizing_mode='stretch_width'
        )
        
        # Create Fleet subtab content — top bar + three-column filters + chart

        # All controls in one row: dates+quick | freq | regions | owners | batteries | analyze
        date_block = pn.Column(
            pn.Row(
                pn.Column("**Start**", self.region_start_date, width=130, margin=(0, 5, 0, 0)),
                pn.Column("**End**", self.region_end_date, width=130, margin=(0, 5, 0, 0)),
            ),
            self.region_date_presets,
            width=270, margin=(0, 10, 0, 0)
        )
        freq_block = pn.Column(
            "**Freq**", self.region_frequency,
            self.region_log_scale,
            width=135, margin=(0, 10, 0, 0)
        )
        region_col = pn.Column(
            "**Regions**", self.region_select_all,
            self.region_checkboxes, width=90,
            margin=(0, 8, 0, 0)
        )
        owner_col = pn.Column(
            "**Owners**", self.owner_select_all,
            self.owner_scroll,
            width=270,
            margin=(0, 8, 0, 0)
        )
        battery_col = pn.Column(
            "**Batteries**", self.battery_select_all,
            self.region_battery_scroll,
            sizing_mode='stretch_width', min_width=300
        )
        analyze_col = pn.Column(
            pn.Spacer(height=18),
            self.region_analyze_button,
            width=145
        )
        controls_row = pn.Row(
            analyze_col,
            date_block, freq_block,
            region_col, owner_col, battery_col,
            height=300, sizing_mode='stretch_width',
            align='start', margin=(0, 0, 0, 0)
        )

        # Inner sub-tabs: Time Series + Time of Day
        region_ts_content = pn.Row(
            self.region_chart_pane,
            pn.Spacer(width=20),
            pn.Column(self.region_info_pane, width=400),
            sizing_mode='stretch_width'
        )

        region_tod_content = pn.Row(
            self.region_tod_chart_pane,
            pn.Spacer(width=20),
            pn.Column(self.region_tod_info_pane, width=350),
            sizing_mode='stretch_width'
        )

        region_views = pn.Tabs(
            ('Time Series', region_ts_content),
            ('Time of Day', region_tod_content),
            sizing_mode='stretch_both'
        )

        region_tab = pn.Column(
            controls_row,
            region_views,
            sizing_mode='stretch_both',
            margin=(0, 0, 0, 0)
        )

        # Create subtabs
        battery_subtabs = pn.Tabs(
            ('Overview', overview_tab),
            ('Fleet', region_tab),
            sizing_mode='stretch_both'
        )

        return battery_subtabs