"""
Curtailment module for AEMO Energy Dashboard
DuckDB-based curtailment analysis
"""

from .curtailment_tab import create_curtailment_tab

__all__ = ['create_curtailment_tab']