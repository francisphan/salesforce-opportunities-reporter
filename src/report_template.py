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
    owner_name: str,
) -> tuple[str, str]:
    """Return (subject, html_body) for a personalized report email."""
    stale = [o for o in opportunities if o.get("_is_stale")]
    active = [o for o in opportunities if not o.get("_is_stale")]
    count = len(opportunities)
    subject = f"Weekly Opportunity Report - {report_date} ({count} opportunities)"

    if not opportunities:
        html = _render_empty(report_date, owner_name)
    else:
        html = _render_full(stale, active, report_date, instance_url, owner_name)

    return subject, html


def _render_empty(report_date: str, owner_name: str) -> str:
    return f"""\
<div style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
  <h2 style="color: #333;">Weekly Opportunity Activity Report</h2>
  <p style="color: #666;">Generated: {report_date}</p>
  <p style="color: #555;">Hi {owner_name},</p>
  {_report_scope()}
  <div style="background: #f0f7ff; border: 1px solid #cce0ff; border-radius: 6px;
              padding: 20px; text-align: center; margin: 20px 0;">
    <p style="color: #555; font-size: 16px; margin: 0;">
      You have no open opportunities with human activity this week.
    </p>
  </div>
  {_footer()}
</div>"""


def _render_full(
    stale: list[dict],
    active: list[dict],
    report_date: str,
    instance_url: str,
    owner_name: str,
) -> str:
    total = len(stale) + len(active)
    sections = []

    if stale:
        sections.append(_render_section(
            stale, instance_url,
            title="Needs Attention — No activity in 2+ months",
            title_color="#c0392b",
            header_bg="#c0392b",
        ))

    if active:
        sections.append(_render_section(
            active, instance_url,
            title="Active Opportunities",
            title_color="#333",
            header_bg="#34495e",
        ))

    sections_html = "\n".join(sections)

    return f"""\
<div style="font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto;">
  <h2 style="color: #333;">Weekly Opportunity Activity Report</h2>
  <p style="color: #666;">Generated: {report_date}</p>
  <p style="color: #555;">Hi {owner_name},</p>
  {_report_scope()}
  <p style="color: #555; font-size: 14px; margin-bottom: 16px;">
    You have <strong>{total}</strong> open opportunit{"y" if total == 1 else "ies"} with human activity.
    {f'<span style="color: #c0392b; font-weight: bold;">{len(stale)} need{"s" if len(stale) == 1 else ""} attention.</span>' if stale else ''}
  </p>
{sections_html}
  {_footer()}
</div>"""


def _render_section(
    opportunities: list[dict],
    instance_url: str,
    title: str,
    title_color: str,
    header_bg: str,
) -> str:
    rows = []
    for i, opp in enumerate(opportunities):
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        opp_url = f"{instance_url}/lightning/r/Opportunity/{opp['Id']}/view"
        name = opp.get("Name", "—")
        account = _get_nested(opp, "Account", "Name") or "—"
        email = _get_nested(opp, "Account", "PersonEmail") or "—"
        language = _get_nested(opp, "Account", "Primary_Language__pc") or "—"
        stage = opp.get("StageName", "—")
        amount = _format_amount(opp.get("Amount"))
        last_touched = opp.get("_last_touched", "N/A")
        touches = opp.get("_touch_count", 0)
        touch_style = "font-weight: bold; color: #d35400;" if touches >= 5 else ""

        rows.append(f"""\
    <tr style="background: {bg};">
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">
        <a href="{opp_url}" style="color: #2a6496; text-decoration: none;">{name}</a>
      </td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{account}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{email}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{language}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{stage}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee; text-align: right;">{amount}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">{last_touched}</td>
      <td style="padding: 8px 12px; border-bottom: 1px solid #eee; text-align: center;{touch_style}">{touches}</td>
    </tr>""")

    rows_html = "\n".join(rows)

    count = len(opportunities)

    return f"""\
  <h3 style="color: {title_color}; margin-top: 24px;">{title} ({count})</h3>
  <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
    <thead>
      <tr style="background: {header_bg}; color: #fff;">
        <th style="padding: 10px 12px; text-align: left;">Opportunity</th>
        <th style="padding: 10px 12px; text-align: left;">Account</th>
        <th style="padding: 10px 12px; text-align: left;">Email</th>
        <th style="padding: 10px 12px; text-align: left;">Language</th>
        <th style="padding: 10px 12px; text-align: left;">Stage</th>
        <th style="padding: 10px 12px; text-align: right;">Amount</th>
        <th style="padding: 10px 12px; text-align: left;">Last Touched</th>
        <th style="padding: 10px 12px; text-align: center;">Touches</th>
      </tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>"""


def _report_scope() -> str:
    return """\
<p style="color: #888; font-size: 13px; font-style: italic; margin-bottom: 16px;">
    This report shows all open opportunities created in the past 6 months (TVG opportunities are excluded).
    Touch counts reflect human interactions (tasks) only — automated system activity is excluded.
  </p>"""


def _footer() -> str:
    return """\
<p style="color: #999; font-size: 12px; margin-top: 24px; border-top: 1px solid #eee; padding-top: 12px;">
    You received this report because you are the owner of the listed opportunities.
    Contact your administrator to unsubscribe.
  </p>"""
