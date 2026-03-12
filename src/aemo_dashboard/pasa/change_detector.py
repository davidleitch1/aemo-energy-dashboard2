"""
Outage Change Detector

Detects changes between outage collection runs:
- New outages appearing
- Outage extensions or shortenings
- Cancelled outages
- Significant capacity changes

Logs changes to outage_changes.parquet for alerting and historical analysis.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Production paths
DEFAULT_DATA_PATH = Path(os.getenv(
    'AEMO_DATA_PATH',
    '/Users/davidleitch/aemo_production/data'
))


class ChangeType(Enum):
    """Types of outage changes."""
    NEW_OUTAGE = "new_outage"
    EXTENDED = "extended"
    SHORTENED = "shortened"
    CANCELLED = "cancelled"
    CAPACITY_REDUCED = "capacity_reduced"
    CAPACITY_RESTORED = "capacity_restored"
    STATUS_CHANGED = "status_changed"


@dataclass
class OutageChange:
    """Represents a detected change in outage data."""
    detected_at: datetime
    source: str  # 'high_impact', 'mtpasa', 'stpasa'
    change_type: ChangeType
    identifier: str  # DUID or Network Asset
    region: Optional[str]
    description: str
    old_value: Optional[str]
    new_value: Optional[str]
    severity: str  # 'info', 'warning', 'critical'

    def to_dict(self) -> Dict:
        return {
            'detected_at': self.detected_at,
            'source': self.source,
            'change_type': self.change_type.value,
            'identifier': self.identifier,
            'region': self.region,
            'description': self.description,
            'old_value': self.old_value,
            'new_value': self.new_value,
            'severity': self.severity,
        }


class ChangeDetector:
    """Detects and logs changes in outage data."""

    # Thresholds for significant changes
    CAPACITY_CHANGE_THRESHOLD_MW = 100  # MW change to trigger alert
    EXTENSION_DAYS_THRESHOLD = 1  # Days extension to trigger alert

    def __init__(self, data_path: Optional[Path] = None):
        self.data_path = data_path or DEFAULT_DATA_PATH
        self.changes_file = self.data_path / 'outage_changes.parquet'
        self.state_dir = self.data_path / '.change_detector_state'
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _load_previous_state(self, source: str) -> Optional[pd.DataFrame]:
        """Load previous state for a source."""
        state_file = self.state_dir / f'{source}_prev.parquet'
        if state_file.exists():
            try:
                return pd.read_parquet(state_file)
            except Exception as e:
                logger.warning(f"Could not load previous state for {source}: {e}")
        return None

    def _save_current_state(self, source: str, df: pd.DataFrame) -> None:
        """Save current state as previous for next comparison."""
        state_file = self.state_dir / f'{source}_prev.parquet'
        try:
            df.to_parquet(state_file, compression='snappy', index=False)
        except Exception as e:
            logger.warning(f"Could not save state for {source}: {e}")

    def get_recent_changes(
        self,
        hours: int = 24,
        severity: Optional[str] = None,
        source: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get recent changes from the log.

        Args:
            hours: Look back this many hours.
            severity: Filter by severity ('info', 'warning', 'critical').
            source: Filter by source ('high_impact', 'mtpasa', 'stpasa').

        Returns:
            DataFrame of recent changes.
        """
        if not self.changes_file.exists():
            return pd.DataFrame()

        df = pd.read_parquet(self.changes_file)

        if df.empty:
            return df

        # Time filter
        cutoff = pd.Timestamp.now() - pd.Timedelta(hours=hours)
        df = df[df['detected_at'] >= cutoff]

        # Severity filter
        if severity:
            df = df[df['severity'] == severity]

        # Source filter
        if source:
            df = df[df['source'] == source]

        return df.sort_values('detected_at', ascending=False)

    def get_change_summary(self, hours: int = 24) -> Dict:
        """
        Get summary of recent changes.

        Returns:
            Dict with counts by type and severity.
        """
        df = self.get_recent_changes(hours=hours)

        if df.empty:
            return {
                'total': 0,
                'by_type': {},
                'by_severity': {},
                'by_source': {},
            }

        return {
            'total': len(df),
            'by_type': df['change_type'].value_counts().to_dict(),
            'by_severity': df['severity'].value_counts().to_dict(),
            'by_source': df['source'].value_counts().to_dict(),
        }

    def get_return_dates(self, duids: List[str]) -> Dict[str, Optional[datetime]]:
        """
        Look up expected return dates for DUIDs from MT-PASA availability forecast.

        The return date is the first day when availability returns to >= 95% of
        the maximum availability in the forecast window.

        Args:
            duids: List of DUID identifiers to look up.

        Returns:
            Dict mapping DUID -> return date (or None if no recovery in forecast).
        """
        mtpasa_file = self.data_path / 'outages_mtpasa.parquet'
        if not mtpasa_file.exists():
            return {duid: None for duid in duids}

        try:
            mtpasa = pd.read_parquet(mtpasa_file)
        except Exception as e:
            logger.warning(f"Could not load MT-PASA data: {e}")
            return {duid: None for duid in duids}

        # Get latest publish datetime
        latest_publish = mtpasa['PUBLISH_DATETIME'].max()
        latest = mtpasa[mtpasa['PUBLISH_DATETIME'] == latest_publish]

        results = {}
        for duid in duids:
            unit_data = latest[latest['DUID'] == duid].sort_values('DAY')

            if unit_data.empty:
                results[duid] = None
                continue

            # Get max availability in forecast as proxy for full capacity
            max_avail = unit_data['PASAAVAILABILITY'].max()
            first_avail = unit_data['PASAAVAILABILITY'].iloc[0]

            # If already at full capacity, no outage
            if first_avail >= max_avail * 0.95:
                results[duid] = None
                continue

            # Find first date when availability returns to >= 95% of max
            recovered = unit_data[unit_data['PASAAVAILABILITY'] >= max_avail * 0.95]
            if len(recovered) > 0:
                results[duid] = recovered['DAY'].iloc[0].to_pydatetime()
            else:
                results[duid] = None

        return results

    def get_current_generator_outages(self, min_reduction_mw: float = 50) -> pd.DataFrame:
        """
        Get generators currently at reduced capacity from latest PASA data.

        Reads ST-PASA (preferred, hourly) or MT-PASA (fallback, 3-hourly)
        to find generators whose near-term availability is below maximum.
        Unlike the change log, this reflects current state — returned generators
        are automatically excluded.

        Args:
            min_reduction_mw: Minimum MW reduction to include.

        Returns:
            DataFrame with DUID, max_mw, current_mw, reduction_mw columns.
        """
        empty = pd.DataFrame(columns=['DUID', 'max_mw', 'current_mw', 'reduction_mw'])

        stpasa_file = self.data_path / 'outages_stpasa.parquet'
        mtpasa_file = self.data_path / 'outages_mtpasa.parquet'

        # Try ST-PASA first (hourly updates, half-hourly resolution)
        if stpasa_file.exists():
            try:
                df = pd.read_parquet(stpasa_file)
                if not df.empty and 'GENERATION_PASA_AVAILABILITY' in df.columns:
                    result = self._outages_from_stpasa(df, min_reduction_mw)
                    if not result.empty:
                        return result
            except Exception as e:
                logger.warning(f"Could not read ST-PASA: {e}")

        # Fallback to MT-PASA (every 3 hours, daily resolution)
        if mtpasa_file.exists():
            try:
                df = pd.read_parquet(mtpasa_file)
                if not df.empty and 'PASAAVAILABILITY' in df.columns:
                    return self._outages_from_mtpasa(df, min_reduction_mw)
            except Exception as e:
                logger.warning(f"Could not read MT-PASA: {e}")

        return empty

    def _outages_from_stpasa(self, df: pd.DataFrame, min_reduction_mw: float) -> pd.DataFrame:
        """Extract current outages from ST-PASA availability data."""
        now = pd.Timestamp.now()
        horizon = now + pd.Timedelta(hours=48)

        # Filter to near-term forecast
        near = df[
            (df['INTERVAL_DATETIME'] >= now) &
            (df['INTERVAL_DATETIME'] <= horizon)
        ]
        if near.empty:
            # Use most recent data if no future intervals
            latest_time = df['INTERVAL_DATETIME'].max()
            near = df[df['INTERVAL_DATETIME'] >= latest_time - pd.Timedelta(hours=6)]

        if near.empty:
            return pd.DataFrame(columns=['DUID', 'max_mw', 'current_mw', 'reduction_mw'])

        # For each DUID: min availability (worst case) and max capacity
        summary = near.groupby('DUID').agg(
            current_mw=('GENERATION_PASA_AVAILABILITY', 'min'),
            max_mw=('GENERATION_MAX_AVAILABILITY', 'max'),
        ).reset_index()

        summary['reduction_mw'] = summary['max_mw'] - summary['current_mw']
        outages = summary[summary['reduction_mw'] >= min_reduction_mw].copy()
        return outages.sort_values('reduction_mw', ascending=False).reset_index(drop=True)

    def _outages_from_mtpasa(self, df: pd.DataFrame, min_reduction_mw: float) -> pd.DataFrame:
        """Extract current outages from MT-PASA availability data."""
        now = pd.Timestamp.now()
        horizon = now + pd.Timedelta(days=7)

        # Get latest publish and filter to near-term
        latest_pub = df['PUBLISH_DATETIME'].max()
        near = df[
            (df['PUBLISH_DATETIME'] == latest_pub) &
            (df['DAY'] >= now) &
            (df['DAY'] <= horizon)
        ]

        if near.empty:
            return pd.DataFrame(columns=['DUID', 'max_mw', 'current_mw', 'reduction_mw'])

        summary = near.groupby('DUID').agg(
            current_mw=('PASAAVAILABILITY', 'min'),
            max_mw=('MAXAVAIL', 'max'),
        ).reset_index()

        summary['reduction_mw'] = summary['max_mw'] - summary['current_mw']
        outages = summary[summary['reduction_mw'] >= min_reduction_mw].copy()
        return outages.sort_values('reduction_mw', ascending=False).reset_index(drop=True)

    def get_data_timestamps(self) -> Dict[str, datetime]:
        """Get last-updated timestamps for each data source file.

        Returns:
            Dict mapping source name -> datetime of last modification.
        """
        timestamps = {}
        files = {
            'stpasa': self.data_path / 'outages_stpasa.parquet',
            'mtpasa': self.data_path / 'outages_mtpasa.parquet',
            'high_impact': self.data_path / 'outages_high_impact.parquet',
        }
        for source, path in files.items():
            if path.exists():
                timestamps[source] = datetime.fromtimestamp(path.stat().st_mtime)
        return timestamps

    def get_notice_for_duids(self, duids: list) -> Dict[str, str]:
        """Look up notice category for DUIDs from the change log.

        Searches the last 7 days of change events for each DUID.

        Args:
            duids: List of DUID identifiers.

        Returns:
            Dict mapping DUID -> notice category string.
        """
        default = {d: 'unknown' for d in duids}

        if not self.changes_file.exists():
            return default

        try:
            changes = pd.read_parquet(self.changes_file)
        except Exception:
            return default

        if changes.empty or 'notice_category' not in changes.columns:
            return default

        # Filter to outage events in the last 7 days
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
        outage_types = {'new_outage', 'capacity_reduced'}
        mask = (
            (changes['detected_at'] >= cutoff) &
            (changes['change_type'].isin(outage_types))
        )
        recent = changes[mask]

        results = {}
        for duid in duids:
            duid_changes = recent[recent['identifier'] == duid]
            if not duid_changes.empty:
                latest = duid_changes.sort_values('detected_at', ascending=False).iloc[0]
                cat = latest.get('notice_category')
                results[duid] = cat if pd.notna(cat) else 'unknown'
            else:
                results[duid] = 'unknown'

        return results
