"""
Lightweight NEM Dashboard tab with fast loading
"""
import panel as pn
from datetime import datetime, timedelta
import threading

def create_nem_dash_tab(query_manager):
    """Create NEM dashboard tab with progressive loading"""
    
    # Create placeholder layout immediately
    loading_indicator = pn.indicators.LoadingSpinner(
        value=True, size=50, name="Loading NEM Dashboard..."
    )
    
    spot_price_card = pn.Card(
        loading_indicator,
        title="Current Spot Prices",
        width=400, height=300
    )
    
    renewable_card = pn.Card(
        pn.pane.Markdown("Loading..."),
        title="Renewable Generation",
        width=400, height=300
    )
    
    generation_card = pn.Card(
        pn.pane.Markdown("Loading..."),
        title="Generation Overview", 
        width=800, height=400
    )
    
    # Create layout with placeholders
    layout = pn.layout.GridSpec(
        height=800,
        max_width=1200
    )
    
    layout[0, 0:4] = spot_price_card
    layout[0, 4:8] = renewable_card
    layout[1:4, :] = generation_card
    
    # Load components in background
    def load_components():
        try:
            # Load spot prices first (most important)
            from .price_components import create_price_section
            spot_content = create_price_section()
            spot_price_card.objects = [spot_content]
            
            # Then renewable gauge
            from .renewable_gauge import create_renewable_gauge_component
            renewable_content = create_renewable_gauge_component()
            renewable_card.objects = [renewable_content]
            
            # Finally generation overview
            from .generation_overview import create_generation_overview_component
            generation_content = create_generation_overview_component()
            generation_card.objects = [generation_content]
            
        except Exception as e:
            import traceback
            error_msg = f"Error loading components: {str(e)}\n{traceback.format_exc()}"
            layout[:, :] = pn.pane.Markdown(error_msg)
    
    # Start loading in background
    threading.Thread(target=load_components, daemon=True).start()
    
    return layout