/**
 * Netlify event-triggered function: submission-created
 * Fires automatically when a Netlify Form receives a submission.
 * Sends a welcome email to new subscribers from both intake forms
 * ("notify" on the homepage and "deepdive-notify" on deep-dive pages).
 *
 * Transport: prefers ZeptoMail API when ZEPTOMAIL_API_TOKEN is set in
 * Netlify env vars (matching scripts/send_weekly_email.py), and falls back
 * to Zoho SMTP via nodemailer when ZOHO_APP_PASSWORD is set.
 */
import nodemailer from "nodemailer";

export const SUBSCRIBE_FORMS = new Set(["notify", "deepdive-notify"]);

export function buildWelcomeEmail(subscriberEmail: string): {
  subject: string;
  html: string;
  unsubUrl: string;
} {
  const unsubUrl = `https://mungbeans.io/.netlify/functions/unsubscribe?email=${encodeURIComponent(subscriberEmail)}`;
  const subject = "Welcome to mungbeans.io — weekly signals & deep dives";

  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; background-color:#0f0f1a; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#0f0f1a; padding:40px 20px;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;">
          <tr>
            <td style="padding:0 0 26px 0;">
              <span style="font-family:monospace; font-size:28px; font-weight:bold; color:#e2b714;">m</span>
              <span style="font-family:sans-serif; font-size:18px; color:#888; margin-left:8px;">mungbeans.io</span>
            </td>
          </tr>
          <tr>
            <td style="color:#e0e0e0; font-size:16px; line-height:1.6;">
              <p style="margin:0 0 18px 0; font-size:22px; color:#ffffff; font-weight:600;">You're on the list</p>
              <p style="margin:0 0 16px 0;">Every Saturday after the weekly close, you'll get one short email covering what moved below its 200-week moving average and the newest five-tab deep dive.</p>
              <p style="margin:0 0 10px 0; color:#ffffff; font-weight:600;">Where to start on the site:</p>
              <p style="margin:0 0 8px 0; padding-left:14px;">&bull; <a href="https://mungbeans.io/deep-dives/" style="color:#e2b714; text-decoration:none; font-weight:600;">Deep Dives</a> &mdash; forensic single-stock theses with the bear case and live price tracking from publish day</p>
              <p style="margin:0 0 8px 0; padding-left:14px;">&bull; <a href="https://mungbeans.io/stocks/" style="color:#e2b714; text-decoration:none; font-weight:600;">All Stocks</a> &mdash; ~2,000 stocks screened by 200-week line, Bean Score (FCF yield dislocation), insider buys, and WOW convergence</p>
              <p style="margin:0 0 8px 0; padding-left:14px;">&bull; <a href="https://mungbeans.io/microcaps/" style="color:#e2b714; text-decoration:none; font-weight:600;">Micro Caps</a> &mdash; sub-$500M innovation screener with cash runway and patent signals</p>
              <p style="margin:0 0 22px 0; padding-left:14px;">&bull; <a href="https://mungbeans.io/book/" style="color:#e2b714; text-decoration:none; font-weight:600;">The Book</a> &mdash; real positions and calls tracked openly against SPY</p>
              <p style="margin:0 0 26px 0;">
                <a href="https://mungbeans.io/deep-dives/" style="display:inline-block; background-color:#e2b714; color:#1a1a2e; text-decoration:none; padding:12px 26px; border-radius:6px; font-weight:600; font-size:15px;">Read the Latest Deep Dive &rarr;</a>
              </p>
            </td>
          </tr>
          <tr>
            <td style="padding:8px 0 0 0;">
              <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#16162a; border:1px solid #2a2a3e; border-left:2px solid #e2b714; border-radius:8px;">
                <tr>
                  <td style="padding:18px 20px;">
                    <p style="margin:0 0 6px 0; font-family:monospace; font-size:10px; letter-spacing:2px; text-transform:uppercase; color:#888;">tools on the desk &middot; no ads</p>
                    <p style="margin:0 0 10px 0; color:#c9c9d4; font-size:14px; line-height:1.6;">
                      mungbeans.io carries no advertising. The only paid links on the site are tools I use myself:
                      <a href="https://simplywall.st/register?referrer_code=a32b7ed506a963a86c00e64f87045877" style="color:#e2b714; text-decoration:none; font-weight:600;">Simply Wall St</a> (five free visual company reports a month) and
                      <a href="https://app.godelterminal.com/?via=mungbeans" style="color:#e2b714; text-decoration:none; font-weight:600;">Godel Terminal</a> (code <b style="color:#ffffff;">mungbeans</b> for 30% off month one).
                    </p>
                    <p style="margin:0; font-size:12px; color:#888; line-height:1.5;">
                      Full list, disclosures, and what each tool cannot do at <a href="https://mungbeans.io/tools/" style="color:#aaa;">mungbeans.io/tools</a>.
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="padding:36px 0 0 0; border-top:1px solid #2a2a3e; margin-top:36px;">
              <p style="color:#666; font-size:13px; margin:18px 0 0 0;">
                You're receiving this because you signed up at mungbeans.io.<br>
                <a href="${unsubUrl}" style="color:#888;">Unsubscribe</a>
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;

  return { subject, html, unsubUrl };
}

async function sendViaZeptoMail(
  zohoEmail: string,
  apiToken: string,
  subscriberEmail: string,
  subject: string,
  html: string,
  unsubUrl: string,
): Promise<void> {
  const res = await fetch("https://api.zeptomail.com/v1.1/email", {
    method: "POST",
    headers: {
      Authorization: apiToken,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      from: { address: zohoEmail, name: "mungbeans.io" },
      to: [{ email_address: { address: subscriberEmail } }],
      subject,
      htmlbody: html,
      mime_headers: {
        "List-Unsubscribe": `<${unsubUrl}>`,
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
      },
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`ZeptoMail HTTP ${res.status}: ${body}`);
  }
}

async function sendViaSmtp(
  zohoEmail: string,
  zohoPassword: string,
  subscriberEmail: string,
  subject: string,
  html: string,
  unsubUrl: string,
): Promise<void> {
  const transporter = nodemailer.createTransport({
    host: "smtp.zoho.com",
    port: 465,
    secure: true,
    auth: {
      user: zohoEmail,
      pass: zohoPassword,
    },
  });

  await transporter.sendMail({
    from: `"mungbeans.io" <${zohoEmail}>`,
    to: subscriberEmail,
    subject,
    html,
    headers: {
      "List-Unsubscribe": `<${unsubUrl}>`,
      "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    },
  });
}

export default async (req: Request) => {
  const { payload } = await req.json();

  if (!SUBSCRIBE_FORMS.has(payload?.form_name)) {
    console.log(`Ignoring form: ${payload?.form_name}`);
    return;
  }

  const subscriberEmail = payload.data?.email?.trim();
  if (!subscriberEmail) {
    console.log("No email in submission, skipping.");
    return;
  }

  const source = payload.data?.source || payload.form_name;
  console.log(`New subscriber (${payload.form_name} / ${source}): ${subscriberEmail}`);

  const zohoEmail = Netlify.env.get("ZOHO_EMAIL");
  const zeptoToken = Netlify.env.get("ZEPTOMAIL_API_TOKEN");
  const zohoPassword = Netlify.env.get("ZOHO_APP_PASSWORD");

  if (!zohoEmail || (!zeptoToken && !zohoPassword)) {
    console.error("Missing ZOHO_EMAIL and mail credentials (ZEPTOMAIL_API_TOKEN or ZOHO_APP_PASSWORD)");
    return;
  }

  const { subject, html, unsubUrl } = buildWelcomeEmail(subscriberEmail);

  try {
    if (zeptoToken) {
      await sendViaZeptoMail(zohoEmail, zeptoToken, subscriberEmail, subject, html, unsubUrl);
      console.log(`Welcome email sent via ZeptoMail to ${subscriberEmail}`);
    } else if (zohoPassword) {
      await sendViaSmtp(zohoEmail, zohoPassword, subscriberEmail, subject, html, unsubUrl);
      console.log(`Welcome email sent via Zoho SMTP to ${subscriberEmail}`);
    }
  } catch (err) {
    console.error(`Failed to send welcome email: ${err}`);
  }
};
