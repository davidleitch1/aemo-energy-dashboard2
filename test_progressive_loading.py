#!/usr/bin/env python3
"""
Test progressive loading approach without defer_load
"""
import panel as pn
import time
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set environment
os.environ.setdefault('AEMO_DASHBOARD_ENV', 'development')

def test_progressive_loading():
    """Test the progressive loading components"""
    
    print("Testing progressive loading approach...")
    print("=" * 60)
    
    # Import progressive components
    from aemo_dashboard.nem_dash.price_components_progressive import create_price_section
    
    # Configure Panel
    pn.extension('tabulator', 'plotly', template='material')
    pn.config.theme = 'dark'
    
    # Create test app
    def create_test_app():
        # Create dashboard with progressive loading
        dashboard = pn.template.MaterialTemplate(
            title="Progressive Loading Test",
            theme=pn.template.DarkTheme
        )
        
        # Add price section with progressive loading
        price_section = create_price_section()
        
        # Create simple layout
        content = pn.Column(
            "# Progressive Loading Test",
            "This tests loading without defer_load to avoid Safari refresh issues.",
            pn.layout.Divider(),
            price_section,
            sizing_mode='stretch_width'
        )
        
        dashboard.main.append(content)
        
        return dashboard
    
    return create_test_app


if __name__ == "__main__":
    print("Starting progressive loading test server...")
    print("Navigate to: http://localhost:5009")
    print("Test browser refresh to ensure no hanging")
    print("Press Ctrl+C to stop")
    
    app = test_progressive_loading()
    pn.serve(app, port=5009, show=False)