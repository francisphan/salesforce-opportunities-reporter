"""One-off script: send a nicely formatted email about Gmail Delegate Access."""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from src.email_sender import send_report

SUBJECT = "Gmail Delegate Access — Everything You Need to Know"

HTML_BODY = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #1a1a1a; line-height: 1.6; margin: 0; padding: 0; background: #f4f4f7; }
  .wrapper { max-width: 640px; margin: 0 auto; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .header { background: linear-gradient(135deg, #4285F4, #34A853); padding: 32px 40px; color: #ffffff; }
  .header h1 { margin: 0; font-size: 24px; font-weight: 600; }
  .header p { margin: 8px 0 0; opacity: 0.9; font-size: 14px; }
  .content { padding: 32px 40px; }
  h2 { color: #4285F4; font-size: 18px; margin: 28px 0 12px; border-bottom: 2px solid #e8eaed; padding-bottom: 6px; }
  h2:first-child { margin-top: 0; }
  p, li { font-size: 15px; color: #3c4043; }
  ul, ol { padding-left: 24px; }
  li { margin-bottom: 6px; }
  .step-number { display: inline-block; background: #4285F4; color: #fff; width: 24px; height: 24px; border-radius: 50%; text-align: center; font-size: 13px; line-height: 24px; margin-right: 8px; font-weight: 600; }
  .steps li { list-style: none; margin-bottom: 10px; }
  .steps { padding-left: 0; }
  table { width: 100%; border-collapse: collapse; margin: 16px 0; font-size: 14px; }
  th { background: #f1f3f4; text-align: left; padding: 10px 14px; font-weight: 600; color: #3c4043; }
  td { padding: 10px 14px; border-bottom: 1px solid #e8eaed; color: #5f6368; }
  tr:last-child td { border-bottom: none; }
  .callout { background: #e8f0fe; border-left: 4px solid #4285F4; padding: 14px 18px; border-radius: 0 6px 6px 0; margin: 16px 0; font-size: 14px; color: #174ea6; }
  .callout.warn { background: #fef7e0; border-left-color: #f9ab00; color: #8a6d00; }
  .limits li { color: #5f6368; }
  code { background: #f1f3f4; padding: 2px 6px; border-radius: 4px; font-size: 13px; color: #d93025; }
  .footer { background: #f8f9fa; padding: 20px 40px; text-align: center; font-size: 12px; color: #9aa0a6; border-top: 1px solid #e8eaed; }
</style>
</head>
<body>
<div class="wrapper">

  <div class="header">
    <h1>Gmail Delegate Access</h1>
    <p>A complete guide to letting someone send email on your behalf</p>
  </div>

  <div class="content">

    <h2>What Is It?</h2>
    <p>Gmail delegation lets you grant another person (or a Google account used by a bot)
    the ability to <strong>read, send, and delete</strong> emails in your Gmail &mdash;
    without sharing your password. Sent messages appear as
    <em>"sent by delegate on behalf of you"</em> in most email clients.</p>

    <h2>How to Set It Up (Personal Gmail)</h2>
    <ol class="steps">
      <li><span class="step-number">1</span> Open <strong>Settings</strong> (gear icon) &rarr; <strong>See all settings</strong></li>
      <li><span class="step-number">2</span> Go to the <strong>Accounts and Import</strong> tab</li>
      <li><span class="step-number">3</span> Under <strong>"Grant access to your account"</strong>, click <strong>"Add another account"</strong></li>
      <li><span class="step-number">4</span> Enter the delegate's email address</li>
      <li><span class="step-number">5</span> The delegate receives a confirmation email and must accept</li>
    </ol>

    <div class="callout">
      <strong>Google Workspace?</strong> Admins can configure delegation via the
      <strong>Admin Console</strong> or the <strong>Gmail API</strong>
      (<code>Users.settings.delegates</code>) and control whether external delegation is allowed.
    </div>

    <h2>How It Appears to Recipients</h2>
    <ul>
      <li>Most clients show: <strong>"sent by delegate@example.com on behalf of you@example.com"</strong></li>
      <li>Some clients display only your name (behavior varies by client)</li>
    </ul>

    <h2>Limits &amp; Restrictions</h2>
    <ul class="limits">
      <li>Maximum <strong>10 delegates</strong> per account</li>
      <li>Delegates <strong>cannot</strong> change your password or account settings</li>
      <li>Delegates <strong>cannot</strong> chat on your behalf</li>
      <li>The delegate must be in the <strong>same Workspace domain</strong>, unless the admin allows external delegation</li>
      <li>For personal Gmail, the delegate must also have a <strong>Gmail / Google account</strong></li>
    </ul>

    <h2>Delegate Access vs. "Send As"</h2>
    <table>
      <tr><th>Feature</th><th>Delegate Access</th><th>"Send As" (Alias)</th></tr>
      <tr><td>Read inbox</td><td>Yes</td><td>No</td></tr>
      <tr><td>Shows delegate name</td><td>Yes (usually)</td><td>No &mdash; looks like it's from you</td></tr>
      <tr><td>Requires other account</td><td>Yes</td><td>No &mdash; just an address you own</td></tr>
      <tr><td>Best for</td><td>Assistant managing your inbox</td><td>Sending from a secondary address</td></tr>
    </table>

    <h2>Programmatic / Bot Access</h2>
    <p>For bots, the recommended approach is <strong>domain-wide delegation via a service account</strong>
    (requires Google Workspace + admin setup):</p>
    <ul>
      <li>Admin grants the service account the <code>gmail.send</code> scope</li>
      <li>The service account impersonates your user via the Gmail API</li>
      <li>Emails are sent from your real address &mdash; no delegate banner</li>
    </ul>

    <div class="callout warn">
      <strong>Note:</strong> Domain-wide delegation requires <strong>Google Workspace</strong>
      (not free Gmail) and must be configured by an organization admin.
    </div>

    <h2>When to Use It</h2>
    <ul>
      <li>A human assistant triaging and replying to your email</li>
      <li>A bot that needs to read incoming mail <strong>and</strong> reply from your address</li>
      <li>Temporary coverage while you're out of office</li>
    </ul>

  </div>
</div>
</body>
</html>
"""

if __name__ == "__main__":
    send_report(
        subject=SUBJECT,
        html_body=HTML_BODY,
        recipients=["bryan@the-vines.com"],
    )
    print("Done!")
