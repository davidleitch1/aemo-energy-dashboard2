"""
Evening Peak Analysis tab for the AEMO dashboard.

Renders a 4-panel matplotlib figure (PCP vs current fuel mix + price + waterfall)
across a configurable region and period, at the evening peak 17:00-22:00.
"""

import logging
from datetime import timedelta

import matplotlib.pyplot as plt
import panel as pn
import param

from .evening_analysis import (
    FLEXOKI,
    create_comparison_figure,
    get_evening_data,
    get_latest_data_date,
)

logger = logging.getLogger(__name__)


class EveningPeakDashboard(param.Parameterized):
    """Parameter-driven view of evening peak fuel mix vs PCP."""

    region = param.Selector(
        default="NEM",
        objects=["NEM", "NSW1", "QLD1", "VIC1", "SA1", "TAS1"],
        doc="Region to analyze",
    )

    period_days = param.Integer(
        default=30,
        bounds=(7, 365),
        doc="Number of days to analyze",
    )

    status = param.String(default="Ready")

    def __init__(self, **params):
        super().__init__(**params)
        self._figure = None

    @param.depends("region", "period_days")
    def view(self):
        self.status = f"Loading {self.region}, {self.period_days} days…"

        try:
            end_date = get_latest_data_date()
            end_exclusive = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
            start_str = (end_date - timedelta(days=self.period_days - 1)).strftime("%Y-%m-%d")

            pcp_end = end_date - timedelta(days=365)
            pcp_end_exclusive = (pcp_end + timedelta(days=1)).strftime("%Y-%m-%d")
            pcp_start = (pcp_end - timedelta(days=self.period_days - 1)).strftime("%Y-%m-%d")

            ly_data, ly_prices, ly_stats = get_evening_data(pcp_start, pcp_end_exclusive, self.region)
            ty_data, ty_prices, ty_stats = get_evening_data(start_str, end_exclusive, self.region)

            if self._figure is not None:
                plt.close(self._figure)

            self._figure = create_comparison_figure(
                ty_data, ty_prices, ty_stats,
                ly_data, ly_prices, ly_stats,
                self.region, self.period_days,
                start_str, end_exclusive,
                pcp_start, pcp_end_exclusive,
            )

            self.status = f"Showing {self.region} — {self.period_days} days"
            return pn.pane.Matplotlib(self._figure, tight=True, dpi=100)

        except Exception as e:
            logger.exception("evening peak view failed")
            self.status = f"Error: {e}"
            return pn.pane.Markdown(f"**Error loading data:** {e}")

    @param.depends("status")
    def status_view(self):
        return pn.pane.Markdown(f"*{self.status}*", styles={"color": FLEXOKI["muted"]})


def create_evening_peak_tab():
    """Build the Evening peak tab layout (embeddable — no template)."""
    dashboard = EveningPeakDashboard()

    region_select = pn.widgets.Select.from_param(
        dashboard.param.region,
        name="Region",
    )

    period_slider = pn.widgets.IntSlider.from_param(
        dashboard.param.period_days,
        name="Period (days)",
        start=7,
        end=365,
        step=1,
    )

    preset_options = {
        "7 days": 7,
        "30 days": 30,
        "90 days": 90,
        "180 days": 180,
        "365 days": 365,
    }

    preset_radio = pn.widgets.RadioBoxGroup(
        options=list(preset_options.keys()),
        value="30 days",
        inline=False,
    )

    def _set_period(event):
        if event.new:
            dashboard.period_days = preset_options[event.new]

    preset_radio.param.watch(_set_period, "value")

    def _sync_preset(event):
        for label, days in preset_options.items():
            if days == event.new:
                preset_radio.value = label
                return
        preset_radio.value = None

    dashboard.param.watch(_sync_preset, "period_days")

    sidebar = pn.Column(
        pn.pane.Markdown("### Settings", styles={"color": FLEXOKI["foreground"]}),
        region_select,
        pn.layout.Divider(),
        pn.pane.Markdown("#### Quick Select", styles={"color": FLEXOKI["text"]}),
        preset_radio,
        pn.layout.Divider(),
        pn.pane.Markdown("#### Custom Period", styles={"color": FLEXOKI["text"]}),
        period_slider,
        pn.layout.Divider(),
        dashboard.status_view,
        width=240,
        styles={"background": FLEXOKI["background"]},
    )

    main = pn.Column(
        pn.pane.Markdown(
            "## Evening Peak Analysis\n"
            "Compare evening peak (17:00–22:00) fuel mix and prices against the "
            "prior comparable period (same dates, one year ago).",
            styles={"color": FLEXOKI["foreground"]},
        ),
        dashboard.view,
        sizing_mode="stretch_width",
        styles={"background": FLEXOKI["background"]},
    )

    return pn.Row(sidebar, main, sizing_mode="stretch_width")
