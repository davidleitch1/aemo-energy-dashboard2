"""
Daily Summary Component for Today tab
Shows 24-hour metrics with automated insights
"""

import pandas as pd
import panel as pn
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

from ..shared.logging_config import get_logger
from ..shared import adapter_selector
from ..generation.generation_query_manager import GenerationQueryManager

logger = get_logger(__name__)

# Initialize query manager for generation data
generation_manager = GenerationQueryManager()

# Regions to include
REGIONS = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']

# Insight templates for automated commentary
INSIGHT_TEMPLATES = {
    'price_up_major': "📈 NEM prices up {pct:.0f}% from yesterday",
    'price_up_minor': "💰 Prices {pct:.0f}% higher than yesterday",
    'price_down_major': "📉 NEM prices down {pct:.0f}% from yesterday", 
    'price_down_minor': "💵 Prices {pct:.0f}% lower than yesterday",
    'price_year_up': "📊 Prices {pct:.0f}% higher than same day last year",
    'price_year_down': "📊 Prices {pct:.0f}% lower than same day last year",
    'renewable_high': "🌱 Renewables at {pct:.0f}%, highest this week",
    'renewable_record': "🎯 New renewable record: {pct:.0f}%",
    'renewable_up': "♻️ Renewable generation up {pct:.0f}% from yesterday",
    'renewable_year': "🌿 Renewables {pct:.0f}% higher than last year",
    'coal_decline': "🔥 Coal generation down {pct:.0f}% from last year",
    'gas_spike': "⚡ Gas generation up {pct:.0f}% today",
    'generation_high': "📊 Total generation {gwh:.0f} GWh, up {pct:.0f}%",
    'generation_year_up': "⚡ Generation {pct:.0f}% higher than same day last year",
    'generation_year_down': "📉 Generation {pct:.0f}% lower than same day last year",
    'volatility': "⚡ Price volatility: ${low:.0f} to ${high:.0f}/MWh"
}


def calculate_daily_metrics(start_time: datetime, end_time: datetime) -> Dict:
    """
    Calculate all metrics for a 24-hour period
    
    Returns dict with structure:
    {
        'prices': {'NSW1': {'avg': 85.2, 'high': 150.5, 'low': 45.3}, ...},
        'generation': {'NSW1': {'total_gwh': 234.5, 'renewable_pct': 35.2, 
                               'gas_pct': 25.3, 'coal_pct': 39.5}, ...},
        'nem_avg_price': 82.5,
        'nem_high_price': 180.0,
        'nem_low_price': 38.0
    }
    """
    try:
        metrics = {
            'prices': {},
            'generation': {},
            'nem_avg_price': 0,
            'nem_high_price': 0,
            'nem_low_price': float('inf')
        }
        
        # Get price data for all regions using adapter
        price_data = adapter_selector.load_price_data(
            start_date=start_time,
            end_date=end_time,
            resolution='30min'
        )
        
        if price_data.empty:
            logger.warning(f"No price data for period {start_time} to {end_time}")
            return metrics
        
        # Standardize column names
        if 'REGIONID' in price_data.columns:
            region_col = 'REGIONID'
            price_col = 'RRP'
        else:
            region_col = 'regionid'
            price_col = 'rrp'
        
        # Calculate price metrics by region
        total_generation = 0
        weighted_price_sum = 0
        
        for region in REGIONS:
            region_prices = price_data[price_data[region_col] == region][price_col]
            
            if not region_prices.empty:
                avg_price = region_prices.mean()
                high_price = region_prices.max()
                low_price = region_prices.min()
                
                metrics['prices'][region] = {
                    'avg': int(round(avg_price)),
                    'high': int(round(high_price)),
                    'low': int(round(low_price))
                }
                
                # Track NEM high/low
                metrics['nem_high_price'] = max(metrics['nem_high_price'], high_price)
                metrics['nem_low_price'] = min(metrics['nem_low_price'], low_price)
        
        # Get generation data using generation manager
        # Try getting NEM-wide data first
        gen_data = generation_manager.query_generation_by_fuel(
            start_date=start_time,
            end_date=end_time,
            region='NEM',  # Try NEM instead of None
            resolution='30min'
        )
        
        # Try a different approach - get each region's data separately
        for region in REGIONS:
            try:
                # Get generation data for this specific region
                region_gen = generation_manager.query_generation_by_fuel(
                    start_date=start_time,
                    end_date=end_time,
                    region=region,
                    resolution='30min'
                )
                
                if not region_gen.empty:
                    logger.info(f"Got generation data for {region}: {len(region_gen)} records")
                    
                    # Calculate total generation in GWh
                    # The data is already time-averaged MW values for each 30-min period
                    # Sum all fuel types, then take average to get average MW
                    # Then convert to GWh: avg_MW * 24 hours / 1000
                    avg_mw_by_fuel = region_gen.groupby('fuel_type')['total_generation_mw'].mean()
                    total_avg_mw = avg_mw_by_fuel.sum()
                    total_gwh = total_avg_mw * 24 / 1000
                    
                    # Calculate fuel shares
                    renewable_fuels = ['Wind', 'Solar', 'Water']  # Water = Hydro
                    gas_fuels = ['Gas', 'Gas (CCGT)', 'Gas (OCGT)', 'Gas (Steam)', 'CCGT', 'OCGT', 'Gas other']
                    coal_fuels = ['Black Coal', 'Brown Coal', 'Coal']
                    
                    # Get average MW for each fuel category
                    renewable_mw = avg_mw_by_fuel[avg_mw_by_fuel.index.isin(renewable_fuels)].sum()
                    gas_mw = avg_mw_by_fuel[avg_mw_by_fuel.index.isin(gas_fuels)].sum()
                    coal_mw = avg_mw_by_fuel[avg_mw_by_fuel.index.isin(coal_fuels)].sum()
                    
                    # Add rooftop solar
                    rooftop_data = adapter_selector.load_rooftop_data(
                        start_date=start_time,
                        end_date=end_time
                    )
                    
                    if rooftop_data is not None and not rooftop_data.empty and region in rooftop_data.columns:
                        rooftop_avg_mw = rooftop_data[region].mean()
                        if not pd.isna(rooftop_avg_mw) and rooftop_avg_mw > 0:
                            renewable_mw += rooftop_avg_mw
                            total_avg_mw += rooftop_avg_mw
                            # Recalculate total GWh with rooftop
                            total_gwh = total_avg_mw * 24 / 1000
                    
                    metrics['generation'][region] = {
                        'total_gwh': int(round(total_gwh)),
                        'renewable_pct': int(round((renewable_mw / total_avg_mw * 100) if total_avg_mw > 0 else 0)),
                        'gas_pct': int(round((gas_mw / total_avg_mw * 100) if total_avg_mw > 0 else 0)),
                        'coal_pct': int(round((coal_mw / total_avg_mw * 100) if total_avg_mw > 0 else 0))
                    }
                    
                    # For weighted average price calculation
                    if region in metrics['prices']:
                        total_generation += total_gwh
                        weighted_price_sum += metrics['prices'][region]['avg'] * total_gwh
                else:
                    logger.warning(f"No generation data for {region}")
                    
            except Exception as e:
                logger.error(f"Error getting generation for {region}: {e}")
            
        # Calculate NEM totals after processing all regions
        if metrics['generation']:
            nem_total_gwh = sum(m['total_gwh'] for m in metrics['generation'].values())
            if nem_total_gwh > 0:
                nem_renewable = sum(m['total_gwh'] * m['renewable_pct'] / 100 for m in metrics['generation'].values()) / nem_total_gwh * 100
                nem_gas = sum(m['total_gwh'] * m['gas_pct'] / 100 for m in metrics['generation'].values()) / nem_total_gwh * 100
                nem_coal = sum(m['total_gwh'] * m['coal_pct'] / 100 for m in metrics['generation'].values()) / nem_total_gwh * 100
            else:
                nem_renewable = nem_gas = nem_coal = 0
            
            metrics['generation']['NEM'] = {
                'total_gwh': int(round(nem_total_gwh)),
                'renewable_pct': int(round(nem_renewable)),
                'gas_pct': int(round(nem_gas)),
                'coal_pct': int(round(nem_coal))
            }
        
        # Calculate volume-weighted average price if we have generation data
        if total_generation > 0:
            metrics['nem_avg_price'] = int(round(weighted_price_sum / total_generation))
        else:
            # Fallback to simple average if no generation data
            if metrics['prices']:
                total_price = sum(p['avg'] for p in metrics['prices'].values())
                metrics['nem_avg_price'] = int(round(total_price / len(metrics['prices'])))
        
        # Round NEM high/low
        metrics['nem_high_price'] = int(round(metrics['nem_high_price']))
        metrics['nem_low_price'] = int(round(metrics['nem_low_price']))
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error calculating daily metrics: {e}")
        import traceback
        traceback.print_exc()
        return {
            'prices': {},
            'generation': {},
            'nem_avg_price': 0,
            'nem_high_price': 0,
            'nem_low_price': 0
        }


def generate_comparison_insights(today_metrics: Dict, yesterday_metrics: Dict, 
                                last_year_metrics: Dict) -> List[str]:
    """
    Generate up to 2 automated insights based on data comparisons
    
    Returns list of insight strings
    """
    insights = []
    scored_insights = []  # List of (score, insight) tuples
    
    try:
        # Price comparison with yesterday
        if today_metrics.get('nem_avg_price', 0) > 0 and yesterday_metrics.get('nem_avg_price', 0) > 0:
            price_change = ((today_metrics['nem_avg_price'] - yesterday_metrics['nem_avg_price']) 
                           / yesterday_metrics['nem_avg_price'] * 100)
            
            if abs(price_change) > 20:  # Major change
                template = 'price_up_major' if price_change > 0 else 'price_down_major'
                score = abs(price_change)
            elif abs(price_change) > 10:  # Minor change
                template = 'price_up_minor' if price_change > 0 else 'price_down_minor'
                score = abs(price_change) * 0.8
            else:
                template = None
                
            if template:
                insight = INSIGHT_TEMPLATES[template].format(pct=abs(price_change))
                scored_insights.append((score, insight))
        
        # Renewable comparison
        if 'NEM' in today_metrics['generation'] and 'NEM' in yesterday_metrics['generation']:
            today_renewable = today_metrics['generation']['NEM']['renewable_pct']
            yesterday_renewable = yesterday_metrics['generation']['NEM']['renewable_pct']
            
            if today_renewable > yesterday_renewable:
                change = today_renewable - yesterday_renewable
                if change > 5:
                    insight = INSIGHT_TEMPLATES['renewable_up'].format(pct=change)
                    scored_insights.append((change * 2, insight))  # Weight renewable changes higher
                    
            # Check if it's a high point
            if today_renewable > 45:
                insight = INSIGHT_TEMPLATES['renewable_high'].format(pct=today_renewable)
                scored_insights.append((today_renewable / 2, insight))
        
        # Year-over-year comparison
        if today_metrics.get('nem_avg_price', 0) > 0 and last_year_metrics.get('nem_avg_price', 0) > 0:
            year_price_change = ((today_metrics['nem_avg_price'] - last_year_metrics['nem_avg_price']) 
                                / last_year_metrics['nem_avg_price'] * 100)
            
            if abs(year_price_change) > 30:
                template = 'price_year_up' if year_price_change > 0 else 'price_year_down'
                insight = INSIGHT_TEMPLATES[template].format(pct=abs(year_price_change))
                scored_insights.append((abs(year_price_change) * 0.6, insight))
        
        # Generation year-over-year comparison
        if ('NEM' in today_metrics.get('generation', {}) and 
            'NEM' in last_year_metrics.get('generation', {})):
            today_gen = today_metrics['generation']['NEM'].get('total_gwh', 0)
            last_year_gen = last_year_metrics['generation']['NEM'].get('total_gwh', 0)
            
            if today_gen > 0 and last_year_gen > 0:
                gen_change = ((today_gen - last_year_gen) / last_year_gen * 100)
                
                if abs(gen_change) > 5:  # Significant change threshold
                    template = 'generation_year_up' if gen_change > 0 else 'generation_year_down'
                    insight = INSIGHT_TEMPLATES[template].format(pct=abs(gen_change))
                    scored_insights.append((abs(gen_change) * 0.7, insight))
        
        # Volatility check
        if today_metrics['nem_high_price'] > 0 and today_metrics['nem_low_price'] > 0:
            price_range = today_metrics['nem_high_price'] - today_metrics['nem_low_price']
            if price_range > 100:
                insight = INSIGHT_TEMPLATES['volatility'].format(
                    low=today_metrics['nem_low_price'],
                    high=today_metrics['nem_high_price']
                )
                scored_insights.append((price_range / 10, insight))
        
        # Sort by score and take top 2
        scored_insights.sort(key=lambda x: x[0], reverse=True)
        insights = [insight for score, insight in scored_insights[:2]]
        
        # If no insights generated, add a basic price summary
        if not insights and today_metrics.get('nem_avg_price', 0) > 0:
            insights.append(f"💰 Average NEM price: ${today_metrics['nem_avg_price']}/MWh")
            
    except Exception as e:
        logger.error(f"Error generating insights: {e}")
        # Provide a fallback insight
        if today_metrics.get('nem_avg_price', 0) > 0:
            insights = [f"💰 Average NEM price: ${today_metrics['nem_avg_price']}/MWh"]
    
    return insights


def create_summary_table(metrics: Dict, insights: List[str]) -> str:
    """
    Create HTML table with metrics and insights
    """
    try:
        html = """
        <div style="width: 100%; padding: 10px; background-color: #282a36; border-radius: 5px;">
            <h4 style="color: #50fa7b; margin: 0 0 10px 0; font-size: 14px;">Daily Summary (Last 24 Hours)</h4>
            <table style="width: 100%; border-collapse: collapse; font-size: 11px; color: #f8f8f2;">
                <thead>
                    <tr style="border-bottom: 1px solid #44475a;">
                        <th style="text-align: left; padding: 4px; color: #8be9fd;"></th>
        """
        
        # Add region headers
        for region in REGIONS + ['NEM']:
            html += f'<th style="text-align: right; padding: 4px; color: #8be9fd;">{region.replace("1", "")}</th>'
        
        html += """
                    </tr>
                </thead>
                <tbody>
        """
        
        # Price rows - only show if we have price data
        has_price_data = bool(metrics.get('prices'))
        has_gen_data = bool(metrics.get('generation'))
        
        if has_price_data:
            price_rows = [
                ('Avg $/MWh', 'prices', 'avg', '#f8f8f2'),
                ('High', 'prices', 'high', '#f8f8f2'),  # White like average
                ('Low', 'prices', 'low', '#f8f8f2'),    # White like average
            ]
        else:
            price_rows = []
            
        if has_gen_data:
            gen_rows = [
                ('Gen GWh', 'generation', 'total_gwh', '#f1fa8c'),
                ('Renew %', 'generation', 'renewable_pct', '#50fa7b'),
                ('Gas %', 'generation', 'gas_pct', '#ffb86c'),
                ('Coal %', 'generation', 'coal_pct', '#ff5555')
            ]
        else:
            gen_rows = []
        
        row_configs = price_rows + gen_rows
        
        for label, category, field, color in row_configs:
            html += f'<tr><td style="padding: 4px; color: {color};">{label}</td>'
            
            for region in REGIONS:
                if category == 'prices' and region in metrics['prices']:
                    value = metrics['prices'][region].get(field, '-')
                elif category == 'generation' and region in metrics['generation']:
                    value = metrics['generation'][region].get(field, '-')
                else:
                    value = '-'
                    
                html += f'<td style="text-align: right; padding: 4px;">{value}</td>'
            
            # NEM column
            if category == 'prices' and field == 'avg':
                # Only show asterisk if we have generation data (volume-weighted)
                if has_gen_data and 'NEM' in metrics['generation']:
                    value = f"{metrics['nem_avg_price']}*"
                else:
                    value = metrics['nem_avg_price']
            elif category == 'prices' and field == 'high':
                value = metrics['nem_high_price']
            elif category == 'prices' and field == 'low':
                value = metrics['nem_low_price']
            elif category == 'generation' and 'NEM' in metrics['generation']:
                value = metrics['generation']['NEM'].get(field, '-')
            else:
                value = '-'
                
            html += f'<td style="text-align: right; padding: 4px; font-weight: bold;">{value}</td>'
            html += '</tr>'
        
        html += """
                </tbody>
            </table>
        """
        
        # Only show volume-weighted note if we have generation data
        if has_gen_data and 'NEM' in metrics.get('generation', {}):
            html += '<p style="font-size: 9px; color: #6272a4; margin: 5px 0 0 0;">* Volume-weighted average</p>'
        
        # Add insights
        if insights:
            html += '<div style="margin-top: 10px; font-size: 11px;">'
            for insight in insights:
                html += f'<p style="margin: 2px 0; color: #f8f8f2;">{insight}</p>'
            html += '</div>'
        
        html += '</div>'
        
        return html
        
    except Exception as e:
        logger.error(f"Error creating summary table: {e}")
        return f'<div style="padding: 10px;">Error creating summary: {str(e)}</div>'


def create_daily_summary_component():
    """
    Create the daily summary component for the Today tab
    
    Note: Structured to easily add email/SMS subscription feature later
    """
    try:
        logger.info("Creating daily summary component")
        
        # Calculate time periods using proper day boundaries
        # FIX: Use day boundaries instead of rolling 24-hour windows to avoid midnight truncation
        now = datetime.now()
        today = now.date()
        yesterday = today - timedelta(days=1)
        last_year = today - timedelta(days=365)
        
        # Use full day boundaries
        today_start = datetime.combine(yesterday, datetime.min.time())  # Yesterday 00:00
        today_end = datetime.combine(today, datetime.max.time())  # Today 23:59:59
        yesterday_start = datetime.combine(yesterday - timedelta(days=1), datetime.min.time())
        yesterday_end = datetime.combine(yesterday, datetime.max.time())
        last_year_start = datetime.combine(last_year - timedelta(days=1), datetime.min.time())
        last_year_end = datetime.combine(last_year, datetime.max.time())
        
        # Get metrics for all periods
        today_metrics = calculate_daily_metrics(today_start, today_end)
        yesterday_metrics = calculate_daily_metrics(yesterday_start, yesterday_end)
        last_year_metrics = calculate_daily_metrics(last_year_start, last_year_end)
        
        # Generate insights
        insights = generate_comparison_insights(today_metrics, yesterday_metrics, last_year_metrics)
        
        # Create table
        table_html = create_summary_table(today_metrics, insights)
        
        # Return as Panel HTML pane
        return pn.pane.HTML(
            table_html,
            sizing_mode='fixed',
            width=400,
            height=350,
            margin=(5, 5)
        )
        
    except Exception as e:
        logger.error(f"Error creating daily summary component: {e}")
        return pn.pane.HTML(
            f'<div style="padding: 20px; color: #ff5555;">Daily Summary Error: {str(e)}</div>',
            width=400,
            height=350
        )


# Future: Email/SMS subscription function placeholder
def send_morning_summary(recipient_email: Optional[str] = None, 
                        recipient_phone: Optional[str] = None):
    """
    Send daily summary at 8:30 AM
    To be implemented when subscription feature is added
    """
    pass