#!/usr/bin/env python3
"""
Data Validity Check for AEMO Dashboard
Provides comprehensive analysis of data coverage and quality across all parquet files.
"""

import pandas as pd
import pickle
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

class DataValidityChecker:
    """Comprehensive data validity and coverage analysis"""
    
    def __init__(self):
        """Initialize the data validity checker"""
        self.results = {}
        
    def check_generation_data(self) -> Dict:
        """Check generation data validity and coverage"""
        try:
            gen_df = pd.read_parquet(config.gen_output_file)
            gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])
            
            # Load DUID mapping for additional context
            with open(config.gen_info_file, 'rb') as f:
                gen_info = pickle.load(f)
            
            result = {
                'status': 'success',
                'records': len(gen_df),
                'columns': list(gen_df.columns),
                'date_range': {
                    'start': gen_df['settlementdate'].min(),
                    'end': gen_df['settlementdate'].max(),
                    'days': (gen_df['settlementdate'].max() - gen_df['settlementdate'].min()).days
                },
                'unique_duids': gen_df['duid'].nunique() if 'duid' in gen_df.columns else 0,
                'mapped_duids': len(gen_info) if gen_info else 0,
                'generation_range': {
                    'min': gen_df['scadavalue'].min() if 'scadavalue' in gen_df.columns else None,
                    'max': gen_df['scadavalue'].max() if 'scadavalue' in gen_df.columns else None
                },
                'sample_data': gen_df.head(2).to_dict('records') if len(gen_df) > 0 else []
            }
            
            # Check for region information in DUID mapping
            if gen_info and isinstance(gen_info, dict) and len(gen_info) > 0:
                sample_duid = list(gen_info.keys())[0]
                if isinstance(gen_info[sample_duid], dict):
                    result['duid_mapping_fields'] = list(gen_info[sample_duid].keys())
                    result['has_region_mapping'] = 'Region' in gen_info[sample_duid]
                    
            return result
            
        except Exception as e:
            logger.error(f"Error checking generation data: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'file_path': str(config.gen_output_file)
            }
    
    def check_price_data(self) -> Dict:
        """Check price data validity and coverage"""
        try:
            price_df = pd.read_parquet(config.spot_hist_file)
            
            # Handle index vs column for SETTLEMENTDATE
            if 'SETTLEMENTDATE' in price_df.columns:
                date_col = 'SETTLEMENTDATE'
            else:
                price_df = price_df.reset_index()
                date_col = 'SETTLEMENTDATE'
                
            price_df[date_col] = pd.to_datetime(price_df[date_col])
            
            result = {
                'status': 'success',
                'records': len(price_df),
                'columns': list(price_df.columns),
                'date_range': {
                    'start': price_df[date_col].min(),
                    'end': price_df[date_col].max(),
                    'days': (price_df[date_col].max() - price_df[date_col].min()).days
                },
                'regions': sorted(price_df['REGIONID'].unique().tolist()) if 'REGIONID' in price_df.columns else [],
                'price_range': {
                    'min': float(price_df['RRP'].min()) if 'RRP' in price_df.columns else None,
                    'max': float(price_df['RRP'].max()) if 'RRP' in price_df.columns else None
                }
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking price data: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'file_path': str(config.spot_hist_file)
            }
    
    def check_transmission_data(self) -> Dict:
        """Check transmission data validity and coverage"""
        try:
            if not Path(config.transmission_output_file).exists():
                return {
                    'status': 'missing',
                    'error': 'Transmission data file does not exist',
                    'file_path': str(config.transmission_output_file)
                }
                
            trans_df = pd.read_parquet(config.transmission_output_file)
            trans_df['settlementdate'] = pd.to_datetime(trans_df['settlementdate'])
            
            result = {
                'status': 'success',
                'records': len(trans_df),
                'columns': list(trans_df.columns),
                'date_range': {
                    'start': trans_df['settlementdate'].min(),
                    'end': trans_df['settlementdate'].max(),
                    'days': (trans_df['settlementdate'].max() - trans_df['settlementdate'].min()).days
                },
                'interconnectors': sorted(trans_df['interconnectorid'].unique().tolist()) if 'interconnectorid' in trans_df.columns else [],
                'flow_range': {
                    'min': float(trans_df['meteredmwflow'].min()) if 'meteredmwflow' in trans_df.columns else None,
                    'max': float(trans_df['meteredmwflow'].max()) if 'meteredmwflow' in trans_df.columns else None
                }
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking transmission data: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'file_path': str(config.transmission_output_file)
            }
    
    def check_rooftop_solar_data(self) -> Dict:
        """Check rooftop solar data validity and coverage"""
        try:
            if not Path(config.rooftop_solar_file).exists():
                return {
                    'status': 'missing',
                    'error': 'Rooftop solar data file does not exist',
                    'file_path': str(config.rooftop_solar_file)
                }
                
            solar_df = pd.read_parquet(config.rooftop_solar_file)
            solar_df['settlementdate'] = pd.to_datetime(solar_df['settlementdate'])
            
            # Get regional columns (exclude settlementdate)
            region_cols = [col for col in solar_df.columns if col != 'settlementdate']
            
            result = {
                'status': 'success',
                'records': len(solar_df),
                'columns': list(solar_df.columns),
                'date_range': {
                    'start': solar_df['settlementdate'].min(),
                    'end': solar_df['settlementdate'].max(),
                    'days': (solar_df['settlementdate'].max() - solar_df['settlementdate'].min()).days
                },
                'regions': region_cols,
                'generation_range': {
                    'min': float(solar_df[region_cols].min().min()) if region_cols else None,
                    'max': float(solar_df[region_cols].max().max()) if region_cols else None
                }
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking rooftop solar data: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'file_path': str(config.rooftop_solar_file)
            }
    
    def analyze_data_coverage(self) -> Dict:
        """Analyze data coverage overlap and identify gaps"""
        gen_result = self.results.get('generation', {})
        price_result = self.results.get('price', {})
        trans_result = self.results.get('transmission', {})
        solar_result = self.results.get('rooftop_solar', {})
        
        coverage_analysis = {
            'datasets_available': [],
            'datasets_missing': [],
            'date_ranges': {},
            'common_period': None,
            'gaps_identified': [],
            'recommendations': []
        }
        
        # Check which datasets are available
        for dataset, result in [
            ('generation', gen_result),
            ('price', price_result), 
            ('transmission', trans_result),
            ('rooftop_solar', solar_result)
        ]:
            if result.get('status') == 'success':
                coverage_analysis['datasets_available'].append(dataset)
                coverage_analysis['date_ranges'][dataset] = result['date_range']
            else:
                coverage_analysis['datasets_missing'].append(dataset)
        
        # Find common period if we have overlapping data
        if len(coverage_analysis['datasets_available']) >= 2:
            date_ranges = coverage_analysis['date_ranges']
            starts = [dr['start'] for dr in date_ranges.values()]
            ends = [dr['end'] for dr in date_ranges.values()]
            
            common_start = max(starts)
            common_end = min(ends)
            
            if common_start <= common_end:
                common_days = (common_end - common_start).days
                coverage_analysis['common_period'] = {
                    'start': common_start,
                    'end': common_end,
                    'days': common_days
                }
                
                # Analyze adequacy for different time ranges
                if common_days >= 30:
                    coverage_analysis['recommendations'].append("✅ Sufficient data for all time range options (24h, 7d, 30d)")
                elif common_days >= 7:
                    coverage_analysis['recommendations'].append("⚠️ Sufficient for 24h and 7d analysis, limited for 30d")
                elif common_days >= 1:
                    coverage_analysis['recommendations'].append("⚠️ Only sufficient for 24h analysis")
                else:
                    coverage_analysis['recommendations'].append("❌ Insufficient overlapping data")
        
        # Identify specific gaps
        if trans_result.get('status') != 'success':
            coverage_analysis['gaps_identified'].append("Transmission data missing or insufficient")
            coverage_analysis['recommendations'].append("🔧 Run transmission backfill to collect historical data")
            
        if solar_result.get('status') != 'success':
            coverage_analysis['gaps_identified'].append("Rooftop solar data missing")
            coverage_analysis['recommendations'].append("🔧 Check rooftop solar data collection")
        
        return coverage_analysis
    
    def run_complete_check(self) -> Dict:
        """Run complete data validity check across all datasets"""
        logger.info("Starting comprehensive data validity check...")
        
        # Check each dataset
        self.results['generation'] = self.check_generation_data()
        self.results['price'] = self.check_price_data()
        self.results['transmission'] = self.check_transmission_data()
        self.results['rooftop_solar'] = self.check_rooftop_solar_data()
        
        # Analyze coverage
        self.results['coverage_analysis'] = self.analyze_data_coverage()
        
        # Add summary
        self.results['summary'] = {
            'timestamp': datetime.now(),
            'total_datasets': 4,
            'available_datasets': len(self.results['coverage_analysis']['datasets_available']),
            'missing_datasets': len(self.results['coverage_analysis']['datasets_missing']),
            'overall_status': 'healthy' if len(self.results['coverage_analysis']['datasets_available']) >= 3 else 'needs_attention'
        }
        
        logger.info(f"Data validity check complete: {self.results['summary']['overall_status']}")
        return self.results

def format_check_results(results: Dict) -> str:
    """Format check results for display"""
    output = ["=== AEMO DASHBOARD DATA VALIDITY CHECK ===\n"]
    
    timestamp = results.get('summary', {}).get('timestamp', datetime.now())
    output.append(f"Check performed: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Summary
    summary = results.get('summary', {})
    output.append(f"Overall Status: {summary.get('overall_status', 'unknown').upper()}")
    output.append(f"Available datasets: {summary.get('available_datasets', 0)}/4\n")
    
    # Individual dataset results
    for dataset_name in ['generation', 'price', 'transmission', 'rooftop_solar']:
        dataset = results.get(dataset_name, {})
        output.append(f"{dataset_name.upper()} DATA:")
        
        if dataset.get('status') == 'success':
            dr = dataset.get('date_range', {})
            output.append(f"  ✅ {dataset.get('records', 0):,} records")
            output.append(f"  📅 {dr.get('start', 'N/A')} to {dr.get('end', 'N/A')} ({dr.get('days', 0)} days)")
            
            if dataset_name == 'generation':
                output.append(f"  🏭 {dataset.get('unique_duids', 0)} DUIDs, {dataset.get('mapped_duids', 0)} mapped")
            elif dataset_name == 'price':
                regions = dataset.get('regions', [])
                output.append(f"  🌏 Regions: {', '.join(regions)}")
            elif dataset_name == 'transmission':
                interconnectors = dataset.get('interconnectors', [])
                output.append(f"  🔌 {len(interconnectors)} interconnectors")
            elif dataset_name == 'rooftop_solar':
                regions = dataset.get('regions', [])
                output.append(f"  ☀️ {len(regions)} regions")
                
        elif dataset.get('status') == 'missing':
            output.append(f"  ❌ File not found: {dataset.get('file_path', 'unknown')}")
        else:
            output.append(f"  ❌ Error: {dataset.get('error', 'unknown')}")
        
        output.append("")
    
    # Coverage analysis
    coverage = results.get('coverage_analysis', {})
    output.append("COVERAGE ANALYSIS:")
    
    common_period = coverage.get('common_period')
    if common_period:
        output.append(f"  📊 Common period: {common_period['start'].date()} to {common_period['end'].date()} ({common_period['days']} days)")
    else:
        output.append("  ⚠️ No overlapping data period found")
    
    gaps = coverage.get('gaps_identified', [])
    if gaps:
        output.append("  🚨 Gaps identified:")
        for gap in gaps:
            output.append(f"    - {gap}")
    
    recommendations = coverage.get('recommendations', [])
    if recommendations:
        output.append("  💡 Recommendations:")
        for rec in recommendations:
            output.append(f"    - {rec}")
    
    return "\n".join(output)

if __name__ == "__main__":
    # Command line usage
    checker = DataValidityChecker()
    results = checker.run_complete_check()
    print(format_check_results(results))