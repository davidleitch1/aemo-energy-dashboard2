"""
Lightweight spot price display - loads only recent data
"""
import panel as pn
from datetime import datetime, timedelta

def create_spot_display_lightweight(query_manager):
    """Create lightweight spot price display"""
    try:
        # Load only last 2 hours of data for initial display
        # Note: 2-hour window accounts for QLD/NSW timezone offset during DST
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=2)

        # Get price data via query manager (fast DuckDB query)
        price_data = query_manager.get_price_history(hours=2)

        if price_data.empty:
            # Fallback: Try 4-hour window if 2-hour window returns no data
            price_data = query_manager.get_price_history(hours=4)

        if price_data.empty:
            return pn.pane.Markdown("No recent price data available")
        
        # Get latest prices by region
        latest_prices = price_data.groupby('regionid').last()
        
        # Create simple price display
        price_cards = []
        for region, row in latest_prices.iterrows():
            color = 'success' if row['rrp'] < 100 else 'warning' if row['rrp'] < 300 else 'danger'
            
            card = pn.Card(
                pn.indicators.Number(
                    value=row['rrp'],
                    format='${value:.2f}',
                    font_size='24pt',
                    colors=[(100, 'green'), (300, 'orange'), (float('inf'), 'red')]
                ),
                title=f"{region}",
                width=150,
                header_color=color
            )
            price_cards.append(card)
        
        return pn.Row(*price_cards[:5])  # Show first 5 regions
        
    except Exception as e:
        return pn.pane.Markdown(f"Error loading prices: {str(e)}")


# For backward compatibility
from .display_spot import create_spot_display_lightweight