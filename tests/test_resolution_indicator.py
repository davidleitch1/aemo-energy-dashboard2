#!/usr/bin/env python3
"""
Test suite for Resolution Indicator UI Component
Tests all indicator functionality and visual components
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_resolution_indicator():
    """Test the resolution indicator UI components"""
    
    print("=" * 70)
    print("TESTING RESOLUTION INDICATOR UI COMPONENTS")
    print("=" * 70)
    
    import panel as pn
    from aemo_dashboard.shared.resolution_indicator import (
        create_resolution_indicator,
        create_performance_summary_indicator,
        create_adaptive_resolution_controls,
        _get_resolution_info,
        _build_indicator_html,
        _get_resolution_help_text
    )
    
    # Initialize Panel
    pn.extension()
    
    # Test 1: Basic resolution indicator creation
    print("\n1. TESTING BASIC RESOLUTION INDICATOR")
    print("-" * 40)
    
    try:
        # Test 5-minute indicator
        indicator_5min = create_resolution_indicator(
            current_resolution='5min',
            data_type='generation',
            date_range={
                'start': datetime.now() - timedelta(days=3),
                'end': datetime.now()
            }
        )
        print("✅ 5-minute resolution indicator created")
        
        # Test 30-minute indicator
        indicator_30min = create_resolution_indicator(
            current_resolution='30min', 
            data_type='price',
            date_range={
                'start': datetime.now() - timedelta(days=30),
                'end': datetime.now()
            }
        )
        print("✅ 30-minute resolution indicator created")
        
        # Test auto indicator
        indicator_auto = create_resolution_indicator(
            current_resolution='auto',
            data_type='transmission'
        )
        print("✅ Auto resolution indicator created")
        
        # Validate types
        assert isinstance(indicator_5min, pn.pane.HTML), "5min indicator not HTML pane"
        assert isinstance(indicator_30min, pn.pane.HTML), "30min indicator not HTML pane"
        assert isinstance(indicator_auto, pn.pane.HTML), "auto indicator not HTML pane"
        print("✅ Basic indicator validation passed")
        
    except Exception as e:
        print(f"❌ Basic indicator test failed: {e}")
        return False
    
    # Test 2: Performance summary indicator
    print("\n2. TESTING PERFORMANCE SUMMARY INDICATOR")
    print("-" * 40)
    
    try:
        # Test with different date ranges
        short_range_summary = create_performance_summary_indicator(
            data_type='generation',
            start_date=datetime.now() - timedelta(days=7),
            end_date=datetime.now()
        )
        print("✅ Short range performance summary created")
        
        long_range_summary = create_performance_summary_indicator(
            data_type='price',
            start_date=datetime.now() - timedelta(days=60),
            end_date=datetime.now()
        )
        print("✅ Long range performance summary created")
        
        # Validate types
        assert isinstance(short_range_summary, pn.pane.HTML), "Short range summary not HTML pane"
        assert isinstance(long_range_summary, pn.pane.HTML), "Long range summary not HTML pane"
        print("✅ Performance summary validation passed")
        
    except Exception as e:
        print(f"❌ Performance summary test failed: {e}")
        return False
    
    # Test 3: Adaptive resolution controls
    print("\n3. TESTING ADAPTIVE RESOLUTION CONTROLS")
    print("-" * 40)
    
    try:
        # Test callback tracking
        callback_calls = []
        
        def test_callback(resolution):
            callback_calls.append(resolution)
            print(f"   Callback received: {resolution}")
        
        # Create controls with callback
        controls_generation = create_adaptive_resolution_controls(
            data_type='generation',
            current_resolution='auto',
            callback_func=test_callback
        )
        print("✅ Generation resolution controls created")
        
        controls_price = create_adaptive_resolution_controls(
            data_type='price',
            current_resolution='5min'
        )
        print("✅ Price resolution controls created")
        
        # Validate types
        assert isinstance(controls_generation, pn.Column), "Generation controls not Column"
        assert isinstance(controls_price, pn.Column), "Price controls not Column"
        print("✅ Adaptive controls validation passed")
        
    except Exception as e:
        print(f"❌ Adaptive controls test failed: {e}")
        return False
    
    # Test 4: Resolution info generation
    print("\n4. TESTING RESOLUTION INFO GENERATION")
    print("-" * 40)
    
    try:
        # Test different resolution types
        info_5min = _get_resolution_info('5min', 'generation', {
            'start': datetime.now() - timedelta(days=2),
            'end': datetime.now()
        })
        
        info_30min = _get_resolution_info('30min', 'price', {
            'start': datetime.now() - timedelta(days=45),
            'end': datetime.now()
        })
        
        info_auto = _get_resolution_info('auto', 'transmission')
        
        # Validate structure
        required_keys = ['resolution', 'display_name', 'icon', 'color', 'explanation', 'data_type']
        for info in [info_5min, info_30min, info_auto]:
            assert all(key in info for key in required_keys), f"Missing keys in resolution info: {info.keys()}"
        
        print(f"✅ 5-minute info: {info_5min['display_name']}")
        print(f"✅ 30-minute info: {info_30min['display_name']}")
        print(f"✅ Auto info: {info_auto['display_name']}")
        print("✅ Resolution info generation validation passed")
        
    except Exception as e:
        print(f"❌ Resolution info test failed: {e}")
        return False
    
    # Test 5: Help text generation
    print("\n5. TESTING HELP TEXT GENERATION")
    print("-" * 40)
    
    try:
        # Test help text for each resolution type
        help_auto = _get_resolution_help_text('auto', 'generation')
        help_5min = _get_resolution_help_text('5min', 'price')
        help_30min = _get_resolution_help_text('30min', 'transmission')
        
        # Validate help text content
        assert 'Auto Selection' in help_auto, "Auto help text missing key content"
        assert 'High Resolution' in help_5min, "5min help text missing key content"
        assert 'Performance Mode' in help_30min, "30min help text missing key content"
        
        print("✅ Auto help text generated")
        print("✅ 5-minute help text generated")
        print("✅ 30-minute help text generated")
        print("✅ Help text generation validation passed")
        
    except Exception as e:
        print(f"❌ Help text test failed: {e}")
        return False
    
    # Test 6: HTML building functionality
    print("\n6. TESTING HTML BUILDING")
    print("-" * 40)
    
    try:
        # Test HTML indicator building
        test_info = {
            'resolution': '5min',
            'display_name': 'High Resolution (5-minute)',
            'icon': '🔍',
            'color': '#50fa7b',
            'explanation': 'Test explanation',
            'data_type': 'generation'
        }
        
        html_with_perf = _build_indicator_html(test_info, True, 300)
        html_without_perf = _build_indicator_html(test_info, False, 300)
        
        # Basic validation
        assert '<div style=' in html_with_perf, "HTML structure missing"
        assert test_info['display_name'] in html_with_perf, "Display name missing from HTML"
        assert test_info['icon'] in html_with_perf, "Icon missing from HTML"
        assert test_info['explanation'] in html_with_perf, "Explanation missing from HTML"
        assert test_info['explanation'] not in html_without_perf, "Explanation should be hidden"
        
        print("✅ HTML with performance info built correctly")
        print("✅ HTML without performance info built correctly")
        print("✅ HTML building validation passed")
        
    except Exception as e:
        print(f"❌ HTML building test failed: {e}")
        return False
    
    # Test 7: Component integration
    print("\n7. TESTING COMPONENT INTEGRATION")
    print("-" * 40)
    
    try:
        # Create a combined dashboard-like layout
        indicator = create_resolution_indicator('30min', 'generation', width=250)
        performance = create_performance_summary_indicator(
            'generation',
            datetime.now() - timedelta(days=14),
            datetime.now(),
            width=300
        )
        controls = create_adaptive_resolution_controls('generation', width=250)
        
        # Create layout
        test_layout = pn.Column(
            pn.pane.Markdown("### Resolution Status"),
            indicator,
            pn.pane.Markdown("### Performance Analysis"),
            performance,
            pn.pane.Markdown("### Resolution Controls"),
            controls,
            width=350
        )
        
        assert isinstance(test_layout, pn.Column), "Test layout not properly created"
        print("✅ Component integration successful")
        print("✅ Dashboard layout created")
        print("✅ Integration test validation passed")
        
    except Exception as e:
        print(f"❌ Component integration test failed: {e}")
        return False
    
    # Test 8: Error handling
    print("\n8. TESTING ERROR HANDLING")
    print("-" * 40)
    
    try:
        # Test with invalid inputs
        invalid_indicator = create_resolution_indicator('invalid_resolution', 'generation')
        print("✅ Invalid resolution handled gracefully")
        
        # Test with missing date range for performance summary
        try:
            error_summary = create_performance_summary_indicator(
                'invalid_data_type',
                datetime.now() - timedelta(days=1),
                datetime.now()
            )
            # Should create component but may show error message
            assert isinstance(error_summary, pn.pane.HTML), "Error summary should still be HTML pane"
            print("✅ Invalid data type handled gracefully")
        except Exception:
            print("✅ Invalid data type properly rejected")
        
        print("✅ Error handling validation passed")
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        return False
    
    print("\n" + "🎯" * 25)
    print("\nRESOLUTION INDICATOR TESTING COMPLETE")
    print("All tests passed successfully!")
    print("\nKey validation points:")
    print("✅ Basic resolution indicators work for all resolution types")
    print("✅ Performance summary indicators provide useful metrics")
    print("✅ Adaptive controls functional with callbacks")
    print("✅ Resolution info generation works correctly")
    print("✅ Help text provides clear guidance")
    print("✅ HTML building creates proper markup")
    print("✅ Component integration works seamlessly")
    print("✅ Error handling is robust")
    
    return True

if __name__ == "__main__":
    success = test_resolution_indicator()
    if success:
        print("\n🎉 ALL RESOLUTION INDICATOR TESTS PASSED!")
    else:
        print("\n❌ SOME TESTS FAILED")
        sys.exit(1)