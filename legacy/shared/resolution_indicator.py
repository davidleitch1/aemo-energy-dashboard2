"""
Resolution Indicator UI Component

Provides visual indicators showing users which data resolution is currently being used,
with explanations and performance impact information.
"""

import panel as pn
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from .logging_config import get_logger
from .resolution_manager import resolution_manager

logger = get_logger(__name__)


def create_resolution_indicator(
    current_resolution: str,
    data_type: str,
    date_range: Optional[Dict[str, datetime]] = None,
    show_performance_info: bool = True,
    width: int = 300
) -> pn.pane.HTML:
    """
    Create visual indicator showing current data resolution
    
    Args:
        current_resolution: '5min' or '30min'
        data_type: 'generation', 'price', 'transmission', 'rooftop'
        date_range: Optional dict with 'start' and 'end' datetime
        show_performance_info: Whether to show performance details
        width: Width of the indicator component
        
    Returns:
        Panel HTML component with resolution indicator
    """
    
    # Get resolution info and explanation
    resolution_info = _get_resolution_info(current_resolution, data_type, date_range)
    
    # Build HTML content
    html_content = _build_indicator_html(
        resolution_info, 
        show_performance_info, 
        width
    )
    
    return pn.pane.HTML(
        html_content,
        width=width,
        height=80 if show_performance_info else 50,
        sizing_mode='fixed'
    )


def create_performance_summary_indicator(
    data_type: str,
    start_date: datetime,
    end_date: datetime,
    current_resolution: Optional[str] = None,
    width: int = 400
) -> pn.pane.HTML:
    """
    Create detailed performance summary indicator
    
    Args:
        data_type: Type of data being loaded
        start_date, end_date: Date range for analysis
        current_resolution: Current resolution (if None, will determine optimal)
        width: Width of the component
        
    Returns:
        Panel HTML component with performance summary
    """
    
    try:
        # Get performance recommendation
        recommendation = resolution_manager.get_performance_recommendation(
            start_date, end_date, data_type
        )
        
        # Override with current resolution if provided
        if current_resolution:
            recommendation['optimal_resolution'] = current_resolution
        
        # Build detailed HTML
        html_content = _build_performance_summary_html(recommendation, width)
        
        return pn.pane.HTML(
            html_content,
            width=width,
            height=120,
            sizing_mode='fixed'
        )
        
    except Exception as e:
        logger.error(f"Error creating performance summary: {e}")
        return pn.pane.HTML(
            f"<div style='color: orange;'>Performance info unavailable: {e}</div>",
            width=width,
            height=30
        )


def create_adaptive_resolution_controls(
    data_type: str,
    current_resolution: str = 'auto',
    callback_func: Optional[callable] = None,
    width: int = 350
) -> pn.Column:
    """
    Create user controls for adaptive resolution selection
    
    Args:
        data_type: Type of data these controls affect
        current_resolution: Current resolution setting
        callback_func: Function to call when resolution changes
        width: Width of the control panel
        
    Returns:
        Panel Column with resolution controls
    """
    
    # Resolution selection widget
    resolution_select = pn.widgets.Select(
        name=f"{data_type.title()} Data Resolution",
        options={
            'Auto (Recommended)': 'auto',
            'High Resolution (5-minute)': '5min', 
            'Performance (30-minute)': '30min'
        },
        value=current_resolution,
        width=width-20
    )
    
    # Info panel that updates based on selection
    info_panel = pn.pane.HTML(
        _get_resolution_help_text('auto', data_type),
        width=width-20,
        height=60
    )
    
    # Update info when selection changes
    def update_info(event):
        new_resolution = event.new
        info_panel.object = _get_resolution_help_text(new_resolution, data_type)
        
        # Call callback if provided
        if callback_func:
            callback_func(new_resolution)
    
    resolution_select.param.watch(update_info, 'value')
    
    return pn.Column(
        resolution_select,
        info_panel,
        width=width,
        margin=(5, 5)
    )


def _get_resolution_info(
    resolution: str,
    data_type: str,
    date_range: Optional[Dict[str, datetime]] = None
) -> Dict[str, Any]:
    """Get resolution information and context"""
    
    resolution_display = {
        '5min': 'High Resolution (5-minute)',
        '30min': 'Performance (30-minute)',
        'auto': 'Auto Selection'
    }
    
    resolution_icons = {
        '5min': '🔍',  # High detail
        '30min': '⚡',  # Performance 
        'auto': '🤖'   # Auto
    }
    
    resolution_colors = {
        '5min': '#50fa7b',  # Green
        '30min': '#ff79c6',  # Pink
        'auto': '#8be9fd'   # Cyan
    }
    
    # Generate explanation
    if date_range:
        duration = date_range['end'] - date_range['start']
        duration_days = duration.total_seconds() / (24 * 3600)
        
        if resolution == '5min':
            if duration_days <= 7:
                explanation = "High resolution appropriate for detailed short-term analysis"
            else:
                explanation = "High resolution may be slow for this date range"
        elif resolution == '30min':
            if duration_days <= 7:
                explanation = "Performance mode - faster loading, less detail"
            else:
                explanation = "Performance mode recommended for long date ranges"
        else:
            explanation = "Automatic selection based on date range and performance"
    else:
        explanation = {
            '5min': "High resolution for maximum detail",
            '30min': "Performance mode for faster loading", 
            'auto': "Intelligent selection based on requirements"
        }.get(resolution, "Unknown resolution")
    
    return {
        'resolution': resolution,
        'display_name': resolution_display.get(resolution, resolution),
        'icon': resolution_icons.get(resolution, '❓'),
        'color': resolution_colors.get(resolution, '#f8f8f2'),
        'explanation': explanation,
        'data_type': data_type
    }


def _build_indicator_html(
    resolution_info: Dict[str, Any],
    show_performance_info: bool,
    width: int
) -> str:
    """Build HTML for resolution indicator"""
    
    icon = resolution_info['icon']
    display_name = resolution_info['display_name']
    color = resolution_info['color']
    explanation = resolution_info['explanation']
    
    # Base indicator HTML
    html = f"""
    <div style="
        display: flex;
        align-items: center;
        padding: 8px 12px;
        background: linear-gradient(135deg, #282a36 0%, #44475a 100%);
        border: 1px solid {color};
        border-radius: 6px;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 13px;
        color: #f8f8f2;
        width: {width-20}px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.3);
    ">
        <span style="
            font-size: 16px;
            margin-right: 8px;
        ">{icon}</span>
        
        <div style="flex: 1;">
            <div style="
                font-weight: 600;
                color: {color};
                margin-bottom: 2px;
            ">{display_name}</div>
            
            {"<div style='font-size: 11px; color: #f8f8f2; opacity: 0.8;'>" + explanation + "</div>" if show_performance_info else ""}
        </div>
    </div>
    """
    
    return html


def _build_performance_summary_html(
    recommendation: Dict[str, Any],
    width: int
) -> str:
    """Build HTML for detailed performance summary"""
    
    resolution = recommendation['optimal_resolution']
    duration_days = recommendation.get('duration_days', 0)
    memory_estimates = recommendation.get('memory_estimates', {})
    load_time_estimate = recommendation.get('load_time_estimate', 0)
    explanation = recommendation.get('explanation', '')
    
    # Get resolution info for styling
    resolution_info = _get_resolution_info(resolution, 'data', None)
    color = resolution_info['color']
    icon = resolution_info['icon']
    
    html = f"""
    <div style="
        padding: 10px;
        background: linear-gradient(135deg, #282a36 0%, #44475a 100%);
        border: 1px solid {color};
        border-radius: 8px;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #f8f8f2;
        width: {width-20}px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.3);
    ">
        <div style="
            display: flex;
            align-items: center;
            margin-bottom: 8px;
            font-weight: 600;
            color: {color};
        ">
            <span style="font-size: 16px; margin-right: 8px;">{icon}</span>
            Performance Summary
        </div>
        
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <div style="font-size: 12px;">
                <strong>Date Range:</strong> {duration_days:.1f} days
            </div>
            <div style="font-size: 12px;">
                <strong>Load Time:</strong> ~{load_time_estimate:.1f}s
            </div>
        </div>
        
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <div style="font-size: 12px;">
                <strong>5-min Memory:</strong> {memory_estimates.get('5min', 0):.1f}MB
            </div>
            <div style="font-size: 12px;">
                <strong>30-min Memory:</strong> {memory_estimates.get('30min', 0):.1f}MB
            </div>
        </div>
        
        <div style="
            font-size: 11px;
            color: #f8f8f2;
            opacity: 0.9;
            margin-top: 6px;
            padding-top: 6px;
            border-top: 1px solid #6272a4;
        ">
            {explanation}
        </div>
    </div>
    """
    
    return html


def _get_resolution_help_text(resolution: str, data_type: str) -> str:
    """Get help text for resolution selection"""
    
    help_texts = {
        'auto': f"""
            <div style="font-size: 12px; color: #50fa7b; padding: 8px; background: #282a36; border-radius: 4px;">
                <strong>📊 Auto Selection (Recommended)</strong><br>
                Automatically chooses the best resolution based on your date range:
                <br>• Short ranges (≤14 days): High resolution (5-minute)
                <br>• Long ranges (>14 days): Performance mode (30-minute)
            </div>
        """,
        
        '5min': f"""
            <div style="font-size: 12px; color: #8be9fd; padding: 8px; background: #282a36; border-radius: 4px;">
                <strong>🔍 High Resolution (5-minute)</strong><br>
                Maximum detail for {data_type} data. Best for:
                <br>• Real-time monitoring and short-term analysis
                <br>• Detailed patterns and anomaly detection
                <br>⚠️ May be slower for long date ranges
            </div>
        """,
        
        '30min': f"""
            <div style="font-size: 12px; color: #ff79c6; padding: 8px; background: #282a36; border-radius: 4px;">
                <strong>⚡ Performance Mode (30-minute)</strong><br>
                Faster loading for {data_type} data. Best for:
                <br>• Historical trend analysis and reports
                <br>• Long-term patterns and seasonal analysis
                <br>✅ Up to 6x faster loading, 80% less memory
            </div>
        """
    }
    
    return help_texts.get(resolution, "Unknown resolution mode")


# Testing function
def test_resolution_indicator():
    """Test the resolution indicator components"""
    
    pn.extension()
    
    # Test basic indicator
    indicator = create_resolution_indicator(
        current_resolution='30min',
        data_type='generation',
        date_range={
            'start': datetime.now() - timedelta(days=30),
            'end': datetime.now()
        }
    )
    
    # Test performance summary
    performance_summary = create_performance_summary_indicator(
        data_type='price',
        start_date=datetime.now() - timedelta(days=14),
        end_date=datetime.now()
    )
    
    # Test adaptive controls
    controls = create_adaptive_resolution_controls(
        data_type='generation',
        current_resolution='auto'
    )
    
    # Combine into test layout
    layout = pn.Column(
        pn.pane.Markdown("## Resolution Indicator Tests"),
        pn.pane.Markdown("### Basic Indicator"),
        indicator,
        pn.pane.Markdown("### Performance Summary"),
        performance_summary,
        pn.pane.Markdown("### Adaptive Controls"),
        controls
    )
    
    return layout


if __name__ == "__main__":
    test_layout = test_resolution_indicator()
    test_layout.show()