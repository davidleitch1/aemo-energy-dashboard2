#!/usr/bin/env python3
"""
Debug WebSocket behavior with defer_load
This test adds logging to understand what happens during serialization
"""
import panel as pn
import pandas as pd
import numpy as np
import logging
import time
from bokeh.server.server import Server

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Create loggers for different components
panel_logger = logging.getLogger('panel')
bokeh_logger = logging.getLogger('bokeh')
tornado_logger = logging.getLogger('tornado')
ws_logger = logging.getLogger('websocket')

# Enable all relevant loggers
for logger in [panel_logger, bokeh_logger, tornado_logger]:
    logger.setLevel(logging.DEBUG)


class WebSocketDebugger:
    """Track WebSocket connection lifecycle"""
    
    def __init__(self):
        self.connections = {}
        self.message_count = 0
    
    def on_open(self, handler):
        """Called when WebSocket opens"""
        conn_id = id(handler)
        self.connections[conn_id] = {
            'opened': time.time(),
            'messages': 0,
            'errors': 0
        }
        ws_logger.info(f"WebSocket OPENED: {conn_id}")
    
    def on_message(self, handler, message):
        """Called on WebSocket message"""
        conn_id = id(handler)
        if conn_id in self.connections:
            self.connections[conn_id]['messages'] += 1
        self.message_count += 1
        
        # Log message details (truncated for large messages)
        msg_str = str(message)[:200] + "..." if len(str(message)) > 200 else str(message)
        ws_logger.debug(f"WebSocket MESSAGE [{conn_id}]: {msg_str}")
    
    def on_close(self, handler):
        """Called when WebSocket closes"""
        conn_id = id(handler)
        if conn_id in self.connections:
            conn_info = self.connections[conn_id]
            duration = time.time() - conn_info['opened']
            ws_logger.info(f"WebSocket CLOSED: {conn_id} (duration: {duration:.2f}s, messages: {conn_info['messages']})")
            del self.connections[conn_id]
    
    def on_error(self, handler, error):
        """Called on WebSocket error"""
        conn_id = id(handler)
        if conn_id in self.connections:
            self.connections[conn_id]['errors'] += 1
        ws_logger.error(f"WebSocket ERROR [{conn_id}]: {error}")


def create_test_dashboard(use_defer_load=False):
    """Create a test dashboard with WebSocket debugging"""
    
    # Configure Panel
    if use_defer_load:
        pn.extension('tabulator', defer_load=True, loading_indicator=True)
        title = "WebSocket Debug - WITH defer_load"
        color = 'red'
    else:
        pn.extension('tabulator')
        title = "WebSocket Debug - WITHOUT defer_load"
        color = 'green'
    
    ws_debugger = WebSocketDebugger()
    
    # Create test data
    def create_test_component():
        ws_logger.info("Creating test component...")
        
        # Simulate some processing
        time.sleep(0.2)
        
        dates = pd.date_range(end=pd.Timestamp.now(), periods=100, freq='5min')
        data = pd.DataFrame({
            'Value': np.random.randn(100).cumsum() + 100
        }, index=dates)
        
        return pn.Column(
            pn.pane.Markdown(f"### Component created at {time.strftime('%H:%M:%S')}"),
            pn.pane.DataFrame(data.tail(10).round(2), width=400, height=200),
            name="Test Component"
        )
    
    # Create component based on defer_load setting
    if use_defer_load:
        ws_logger.info("Creating DEFERRED component")
        component = pn.panel(create_test_component, defer_load=True, loading_indicator=True)
    else:
        ws_logger.info("Creating IMMEDIATE component")
        component = create_test_component()
    
    # Create status display
    status = pn.pane.Markdown("""
    ### WebSocket Status
    Waiting for connections...
    """, width=600)
    
    def update_status():
        """Update connection status display"""
        status_text = f"""
        ### WebSocket Status
        **Active Connections**: {len(ws_debugger.connections)}
        **Total Messages**: {ws_debugger.message_count}
        
        **Connection Details**:
        """
        for conn_id, info in ws_debugger.connections.items():
            duration = time.time() - info['opened']
            status_text += f"\n- Connection {conn_id}: {info['messages']} messages, {duration:.1f}s"
        
        status.object = status_text
    
    # Create dashboard
    template = pn.template.MaterialTemplate(
        title=title,
        header_background=color,
    )
    
    template.main.extend([
        pn.pane.Markdown(f"""
        ## WebSocket Debug Test
        
        **Configuration**: defer_load = {use_defer_load}
        
        ### Test Procedure:
        1. Open browser developer tools (F12)
        2. Go to Network tab, filter by WS (WebSocket)
        3. Load this page
        4. Note WebSocket connection established
        5. **Refresh the page** (Cmd+R)
        6. Watch for WebSocket errors or hanging
        
        ### Expected Behavior:
        - **Without defer_load**: Clean reconnection
        - **With defer_load**: May see serialization errors or hang
        """),
        pn.layout.Divider(),
        status,
        pn.layout.Divider(),
        component
    ])
    
    # Add periodic status update
    if hasattr(pn.state, 'add_periodic_callback'):
        pn.state.add_periodic_callback(update_status, period=1000)
    
    return template, ws_debugger


def run_debug_test(use_defer_load=False):
    """Run the WebSocket debug test"""
    dashboard, debugger = create_test_dashboard(use_defer_load)
    
    print(f"\n{'='*60}")
    print(f"WebSocket Debug Test - defer_load={use_defer_load}")
    print(f"{'='*60}")
    print("\nStarting server with detailed logging...")
    print("Navigate to: http://localhost:5011")
    print("\nWatch the console for WebSocket activity")
    print("Test browser refresh to see serialization behavior")
    print("\nPress Ctrl+C to stop")
    
    dashboard.show(port=5011)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "defer":
        run_debug_test(use_defer_load=True)
    else:
        print("\nWebSocket Debug Test")
        print("="*60)
        print("\nUsage:")
        print("  python test_websocket_debug.py         # Test WITHOUT defer_load")
        print("  python test_websocket_debug.py defer   # Test WITH defer_load")
        print("\nThe test will show detailed WebSocket activity in the console")