"""Render the weekly opportunity report as an HTML email."""


def _format_amount(amount) -> str:
    if amount is None:
        return "N/A"
    return f"${amount:,.0f}"


def _format_date(date_str) -> str:
    if not date_str:
        return "N/A"
    return date_str[:10]


def _get_nested(record: dict, *keys, default=""):
    """Safely traverse nested dicts (e.g. Owner.Name from SOQL relationship queries)."""
    val = record
    for key in keys:
        if not isinstance(val, dict):
            return default
        val = val.get(key)
        if val is None:
            return default
    return val


def render_report(
    opportunities: list[dict],
    report_date: str,
    instance_url: str,
) -> tuple[str, str]:
    """Return (subject, html_body) for the report email."""
    count = len(opportunities)
    subject = f"Weekly Opportunity Report - {report_date} ({count} opportunities)"

    if not opportunities:
        html = _render_empty(report_date)
    else:
        html = _render_table(opportunities, report_date, instance_url)

    return subject, html


def _render_empty(report_date: str) -> str:
    return f"""\
<div style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
  <h2 style="color: #333;">Weekly Opportunity Activity Report</h2>
  <p style="color: #666;">Generated: {report_date}</p>
  <div style="background: #f0f7ff; border: 1px solid #cce0ff; border-radius: 6px;
              padding: 20px; text-align: center; margin: 20px 0;">
    <p style="color: #555; font-size: 16px; margin: 0;">
      No open opportunities with 2+ human touches this week.
    </p>
  </div>
  {_footer()}
</div>"""


def _render_table(
    opportunities: list[dict],
    report_date: str,
    instance_url: str,
) -> str:
    rows = []
    for i, opp in enumerate(opportunities):
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        opp_url = f"{instance_url}/lightning/r/Opportunity/{opp['Id']}/view"
        name = opp.get("Name", "—")
        account = _get_nested(opp, "Account", "Name") or "—"
        stage = opp.get("StageName", "—")
        amount = _format_amount(opp.get("Amount"))
        close_date = _format_date(opp.get("CloseDate"))
        owner = _get_nested(opp, "Owner", "Name") or "—"
        touches = opp.get("_touch_count", 0)
        touch_style = "font-weight: bold; color: #d35400;" if touches >= 5 else ""

        rows.append(f"""\
    <tr style="background: {bg};">
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">
        <a href="{opp_url}" style="color: #2a6496; text-decoration: none;">{name}</a>
      </td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{account}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{stage}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee; text-align: right;">{amount}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{close_date}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{owner}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee; text-align: center;{touch_style}">{touches}</td>
    </tr>""")

    rows_html = "\n".join(rows)
    count = len(opportunities)

    return f"""\
<div style="font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto;">
  <h2 style="color: #333;">Weekly Opportunity Activity Report</h2>
  <p style="color: #666;">Generated: {report_date}</p>
  <p style="color: #555; font-size: 14px; margin-bottom: 16px;">
    <strong>{count}</strong> open opportunit{"y" if count == 1 else "ies"} with 2+ human touches.
  </p>
  <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
    <thead>
      <tr style="background: #34495e; color: #fff;">
        <th style="padding: 10px 12px; text-align: left;">Opportunity</th>
        <th style="padding: 10px 12px; text-align: left;">Account</th>
        <th style="padding: 10px 12px; text-align: left;">Stage</th>
        <th style="padding: 10px 12px; text-align: right;">Amount</th>
        <th style="padding: 10px 12px; text-align: left;">Close Date</th>
        <th style="padding: 10px 12px; text-align: left;">Owner</th>
        <th style="padding: 10px 12px; text-align: center;">Touches</th>
      </tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
  {_footer()}
</div>"""


def _footer() -> str:
    return """\
<p style="color: #999; font-size: 12px; margin-top: 24px; border-top: 1px solid #eee; padding-top: 12px;">
    You received this because you are subscribed in <code>subscribers.yaml</code>.
    To unsubscribe, open a PR to remove your email from that file.
  </p>"""
