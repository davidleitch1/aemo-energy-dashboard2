"""
Shared email alert functionality for AEMO Energy Dashboard
"""

import os
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, List, Optional
import logging

from .logging_config import get_logger

logger = get_logger(__name__)

class EmailAlertManager:
    """Manages email alerts with rate limiting and caching"""
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize email alert manager.
        
        Args:
            data_dir: Directory for storing alert cache files
        """
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            data_dir_env = os.getenv('DATA_DIR')
            if data_dir_env:
                self.data_dir = Path(data_dir_env)
            else:
                # Default to data/ in project root
                project_root = Path(__file__).parent.parent.parent.parent
                self.data_dir = project_root / 'data'
        
        self.data_dir.mkdir(exist_ok=True)
        
        # Email configuration
        self.sender_email = os.getenv('ALERT_EMAIL')
        self.sender_password = os.getenv('ALERT_PASSWORD')
        self.recipient_email = os.getenv('RECIPIENT_EMAIL', self.sender_email)
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.mail.me.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        
        # Alert behavior
        self.enabled = os.getenv('ENABLE_EMAIL_ALERTS', 'true').lower() == 'true'
        self.cooldown_hours = int(os.getenv('ALERT_COOLDOWN_HOURS', '24'))
    
    def is_configured(self) -> bool:
        """Check if email is properly configured"""
        return bool(self.sender_email and self.sender_password)
    
    def send_email(self, subject: str, body: str, is_html: bool = False) -> bool:
        """
        Send an email alert.
        
        Args:
            subject: Email subject
            body: Email body content
            is_html: Whether body is HTML content
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.info("Email alerts disabled")
            return False
            
        if not self.is_configured():
            logger.error("Email not configured. Set ALERT_EMAIL and ALERT_PASSWORD environment variables.")
            return False
        
        try:
            # Create email
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = self.recipient_email
            msg['Subject'] = subject
            
            # Attach body
            msg_type = 'html' if is_html else 'plain'
            msg.attach(MIMEText(body, msg_type))
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            logger.info(f"✅ Email sent successfully via {self.smtp_server}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")
            return False
    
    def load_alert_cache(self, cache_name: str) -> Dict:
        """Load alert cache from file"""
        cache_file = self.data_dir / f"{cache_name}.json"
        try:
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.error(f"Error loading alert cache {cache_name}: {e}")
            return {}
    
    def save_alert_cache(self, cache_name: str, cache_data: Dict) -> None:
        """Save alert cache to file"""
        cache_file = self.data_dir / f"{cache_name}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error saving alert cache {cache_name}: {e}")
    
    def should_send_alert(self, cache_name: str, alert_keys: Set[str]) -> Set[str]:
        """
        Check which alerts should be sent based on cooldown.
        
        Args:
            cache_name: Name of the alert cache
            alert_keys: Set of alert keys to check
            
        Returns:
            Set of alert keys that should trigger alerts
        """
        cache = self.load_alert_cache(cache_name)
        now = datetime.now()
        keys_to_alert = set()
        
        for key in alert_keys:
            if key not in cache:
                keys_to_alert.add(key)
            else:
                last_alert = datetime.fromisoformat(cache[key])
                if (now - last_alert).total_seconds() > self.cooldown_hours * 3600:
                    keys_to_alert.add(key)
        
        if keys_to_alert:
            # Update cache for keys we're about to alert
            for key in keys_to_alert:
                cache[key] = now.isoformat()
            self.save_alert_cache(cache_name, cache)
        
        return keys_to_alert
    
    def send_duid_alert(self, unknown_duids: Set[str], duid_data: Dict) -> bool:
        """
        Send alert about unknown DUIDs.
        
        Args:
            unknown_duids: Set of unknown DUID identifiers
            duid_data: Dictionary with DUID sample data
            
        Returns:
            True if email sent successfully
        """
        if not unknown_duids:
            return False
        
        # Check rate limiting
        duids_to_alert = self.should_send_alert('unknown_duids_alerts', unknown_duids)
        if not duids_to_alert:
            logger.info(f"Skipping email - all {len(unknown_duids)} DUIDs recently alerted")
            return False
        
        # Create email content
        subject = f"⚠️ Unknown DUIDs in Energy Dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        body = self._create_duid_alert_body(duids_to_alert, duid_data)
        
        success = self.send_email(subject, body, is_html=True)
        
        if success:
            logger.info(f"Email alert sent for {len(duids_to_alert)} unknown DUIDs")
        
        return success
    
    def _create_duid_alert_body(self, unknown_duids: Set[str], duid_data: Dict) -> str:
        """Create HTML email body for DUID alerts"""
        
        # Get sample data for each unknown DUID
        samples_html = ""
        for duid in sorted(unknown_duids)[:10]:  # Limit to 10 for email size
            if duid in duid_data:
                sample = duid_data[duid]
                samples_html += f"""
                <tr>
                    <td>{duid}</td>
                    <td>{sample.get('power', 0):.1f} MW</td>
                    <td>{sample.get('time', 'Unknown')}</td>
                    <td>{sample.get('records', 0)}</td>
                </tr>
                """
        
        body = f"""
        <html>
        <body>
            <h2>🚨 Unknown DUIDs Detected</h2>
            <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Found <strong>{len(unknown_duids)} unknown DUID(s)</strong> not in gen_info.pkl</p>
            
            <h3>Sample Data (first 10):</h3>
            <table border="1" style="border-collapse: collapse;">
            <tr>
                <th>DUID</th>
                <th>Latest Power</th>
                <th>Latest Time</th>
                <th>Records (24h)</th>
            </tr>
            {samples_html}
            </table>
            
            <h3>Action Required:</h3>
            <ul>
                <li>Update gen_info.pkl with new DUID information</li>
                <li>Check AEMO data sources for DUID details</li>
                <li>Verify these are legitimate new generation units</li>
                <li>Consider adding to exception list if not important</li>
            </ul>
            
            <p><em>All unknown DUIDs: {', '.join(sorted(unknown_duids))}</em></p>
            
            <hr>
            <p><small>Generated by AEMO Energy Dashboard</small></p>
        </body>
        </html>
        """
        return body