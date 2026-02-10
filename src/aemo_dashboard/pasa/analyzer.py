"""
Outage Insights Analyzer

Extracts actionable insights from collected outage data.
Provides structured summaries for dashboard display and alerting.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Production paths
DEFAULT_DATA_PATH = Path(os.getenv(
    'AEMO_DATA_PATH',
    '/Users/davidleitch/aemo_production/data'
))


@dataclass
class OutageSummary:
    """Summary of outage insights."""
    report_date: datetime
    total_outages: int
    in_progress: int
    upcoming_7d: int
    upcoming_30d: int
    unplanned: int
    inter_regional: int
    by_region: Dict[str, int]
    by_status: Dict[str, int]


class OutageAnalyzer:
    """Analyzes outage data to extract key insights."""

    # Valid NEM regions
    REGIONS = {'NSW', 'QLD', 'VIC', 'SA', 'TAS'}

    def __init__(self, data_path: Optional[Path] = None):
        self.data_path = data_path or DEFAULT_DATA_PATH
        self.high_impact_file = self.data_path / 'outages_high_impact.parquet'
        self._df: Optional[pd.DataFrame] = None
        self._load_time: Optional[datetime] = None

    def _load_data(self, force: bool = False) -> pd.DataFrame:
        """Load high impact outage data, with caching."""
        # Reload if forced or cache is stale (>5 min)
        if (force or self._df is None or self._load_time is None or
                (datetime.now() - self._load_time).seconds > 300):
            if self.high_impact_file.exists():
                self._df = pd.read_parquet(self.high_impact_file)
                self._load_time = datetime.now()
                logger.debug(f"Loaded {len(self._df)} outage records")
            else:
                logger.warning(f"No outage data file: {self.high_impact_file}")
                self._df = pd.DataFrame()

        return self._df

    def get_latest_report_date(self) -> Optional[datetime]:
        """Get the date of the most recent report."""
        df = self._load_data()
        if df.empty or 'report_date' not in df.columns:
            return None
        return df['report_date'].max()

    def get_current_outages(self) -> pd.DataFrame:
        """
        Get outages that are currently in progress.

        Returns:
            DataFrame of outages where Start <= now <= Finish
        """
        df = self._load_data()
        if df.empty:
            return pd.DataFrame()

        now = pd.Timestamp.now()

        # Filter to in-progress
        mask = (
            (df['Start'] <= now) &
            (df['Finish'] >= now) &
            df['Region'].notna()
        )

        current = df[mask].copy()

        # Also include those with "In Progress" status
        if 'Status' in df.columns:
            status_mask = df['Status'].str.contains(
                'In Progress|PTP', case=False, na=False
            )
            current = pd.concat([
                current,
                df[status_mask & df['Region'].notna()]
            ]).drop_duplicates()

        return current.sort_values('Start', ascending=False)

    def get_upcoming_outages(self, days: int = 30) -> pd.DataFrame:
        """
        Get planned outages in the next N days.

        Args:
            days: Number of days to look ahead.

        Returns:
            DataFrame of upcoming outages sorted by start date.
        """
        df = self._load_data()
        if df.empty:
            return pd.DataFrame()

        now = pd.Timestamp.now()
        future = now + pd.Timedelta(days=days)

        mask = (
            (df['Start'] >= now) &
            (df['Start'] <= future) &
            df['Region'].notna() &
            ~df['Status'].str.contains('Withdrawn|Cancel', case=False, na=False)
        )

        return df[mask].sort_values('Start')

    def get_inter_regional_outages(self) -> pd.DataFrame:
        """
        Get outages that affect interconnectors.

        These are particularly important for price impact analysis
        as they can constrain power flow between regions.

        Returns:
            DataFrame of inter-regional outages.
        """
        df = self._load_data()
        if df.empty or 'Inter-Regional' not in df.columns:
            return pd.DataFrame()

        now = pd.Timestamp.now()

        mask = (
            df['Inter-Regional'].fillna('').str.strip().str.upper() == 'T'
        ) & (
            df['Finish'] >= now
        ) & (
            df['Region'].notna()
        ) & (
            ~df['Status'].str.contains('Withdrawn|Cancel', case=False, na=False)
        )

        return df[mask].sort_values('Start')

    def get_unplanned_outages(self) -> pd.DataFrame:
        """
        Get unplanned/forced outages.

        These are higher concern as they are unexpected.

        Returns:
            DataFrame of unplanned outages.
        """
        df = self._load_data()
        if df.empty or 'Unplanned?' not in df.columns:
            return pd.DataFrame()

        now = pd.Timestamp.now()

        mask = (
            df['Unplanned?'].fillna('').str.strip().str.upper() == 'T'
        ) & (
            df['Finish'] >= now
        ) & (
            df['Region'].notna()
        )

        return df[mask].sort_values('Start')

    def get_regional_summary(self) -> Dict[str, int]:
        """
        Get count of active outages by region.

        Returns:
            Dict mapping region code to outage count.
        """
        df = self._load_data()
        if df.empty:
            return {r: 0 for r in self.REGIONS}

        now = pd.Timestamp.now()

        # Active = not finished and not withdrawn
        mask = (
            (df['Finish'] >= now) &
            df['Region'].notna() &
            ~df['Status'].str.contains('Withdrawn|Cancel', case=False, na=False)
        )

        active = df[mask]
        counts = active['Region'].value_counts().to_dict()

        # Ensure all regions present
        return {r: counts.get(r, 0) for r in self.REGIONS}

    def get_status_summary(self) -> Dict[str, int]:
        """
        Get count of outages by status.

        Returns:
            Dict mapping status to count.
        """
        df = self._load_data()
        if df.empty or 'Status' not in df.columns:
            return {}

        now = pd.Timestamp.now()
        active = df[df['Finish'] >= now]

        return active['Status'].value_counts().to_dict()

    def get_summary(self) -> OutageSummary:
        """
        Get complete summary of outage insights.

        Returns:
            OutageSummary dataclass with all key metrics.
        """
        df = self._load_data()

        if df.empty:
            return OutageSummary(
                report_date=datetime.now(),
                total_outages=0,
                in_progress=0,
                upcoming_7d=0,
                upcoming_30d=0,
                unplanned=0,
                inter_regional=0,
                by_region={r: 0 for r in self.REGIONS},
                by_status={},
            )

        return OutageSummary(
            report_date=self.get_latest_report_date() or datetime.now(),
            total_outages=len(df[df['Region'].notna()]),
            in_progress=len(self.get_current_outages()),
            upcoming_7d=len(self.get_upcoming_outages(days=7)),
            upcoming_30d=len(self.get_upcoming_outages(days=30)),
            unplanned=len(self.get_unplanned_outages()),
            inter_regional=len(self.get_inter_regional_outages()),
            by_region=self.get_regional_summary(),
            by_status=self.get_status_summary(),
        )

    def format_outage_table(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Format outage DataFrame for display.

        Args:
            df: Raw outage DataFrame.
            columns: Columns to include (default: key columns).

        Returns:
            Formatted DataFrame ready for display.
        """
        if df.empty:
            return pd.DataFrame()

        if columns is None:
            columns = ['Region', 'Network Asset', 'Start', 'Finish', 'Status']

        # Select available columns
        available = [c for c in columns if c in df.columns]
        result = df[available].copy()

        # Format dates
        for col in ['Start', 'Finish']:
            if col in result.columns:
                result[col] = result[col].dt.strftime('%Y-%m-%d %H:%M')

        # Truncate long asset names
        if 'Network Asset' in result.columns:
            result['Network Asset'] = result['Network Asset'].str[:50]

        return result
