/**
 * Netlify event-triggered function: submission-created
 * Fires automatically when a Netlify Form receives a submission.
 * Sends a welcome email to new subscribers via Zoho SMTP.
 */
import nodemailer from "nodemailer";

export default async (req: Request) => {
  const { payload } = await req.json();

  // Only process "notify" form submissions
  if (payload.form_name !== "notify") {
    console.log(`Ignoring form: ${payload.form_name}`);
    return;
  }

  const subscriberEmail = payload.data?.email;
  if (!subscriberEmail) {
    console.log("No email in submission, skipping.");
    return;
  }

  console.log(`New subscriber: ${subscriberEmail}`);

  const zohoEmail = Netlify.env.get("ZOHO_EMAIL");
  const zohoPassword = Netlify.env.get("ZOHO_APP_PASSWORD");

  if (!zohoEmail || !zohoPassword) {
    console.error("Missing ZOHO_EMAIL or ZOHO_APP_PASSWORD env vars");
    return;
  }

  const transporter = nodemailer.createTransport({
    host: "smtp.zoho.com",
    port: 465,
    secure: true,
    auth: {
      user: zohoEmail,
      pass: zohoPassword,
    },
  });

  const welcomeHtml = `
<!DOCTYPE html>
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
          <!-- Header -->
          <tr>
            <td style="padding:0 0 30px 0;">
              <span style="font-family:monospace; font-size:28px; font-weight:bold; color:#e2b714;">m</span>
              <span style="font-family:sans-serif; font-size:18px; color:#888; margin-left:8px;">mungbeans.io</span>
            </td>
          </tr>
          <!-- Body -->
          <tr>
            <td style="color:#e0e0e0; font-size:16px; line-height:1.6;">
              <p style="margin:0 0 20px 0; font-size:22px; color:#ffffff; font-weight:600;">Welcome to mungbeans.io</p>
              <p style="margin:0 0 16px 0;">You're now on the list. Every week, you'll get a short email when stocks cross below their 200-week moving average — a signal that value investors have used for decades to find quality companies at a discount.</p>
              <p style="margin:0 0 16px 0;">Each report includes:</p>
              <p style="margin:0 0 8px 0; padding-left:16px;">• New crossings below the 200-week line</p>
              <p style="margin:0 0 8px 0; padding-left:16px;">• Recoveries back above</p>
              <p style="margin:0 0 16px 0; padding-left:16px;">• Key metrics: RSI, FCF yield, insider activity</p>
              <p style="margin:0 0 24px 0;">Reports go out on Saturdays after the market closes for the week.</p>
              <p style="margin:0 0 24px 0;">In the meantime, check out the current signals:</p>
              <a href="https://mungbeans.io" style="display:inline-block; background-color:#e2b714; color:#1a1a2e; text-decoration:none; padding:12px 28px; border-radius:6px; font-weight:600; font-size:15px;">View This Week's Signals →</a>
            </td>
          </tr>
          <!-- Footer -->
          <tr>
            <td style="padding:40px 0 0 0; border-top:1px solid #2a2a3e; margin-top:40px;">
              <p style="color:#666; font-size:13px; margin:20px 0 0 0;">
                You're receiving this because you signed up at mungbeans.io.<br>
                <a href="https://mungbeans.io/.netlify/functions/unsubscribe?email=${encodeURIComponent(subscriberEmail)}" style="color:#888;">Unsubscribe</a>
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;

  try {
    await transporter.sendMail({
      from: `"mungbeans.io" <${zohoEmail}>`,
      to: subscriberEmail,
      subject: "Welcome to mungbeans.io — weekly 200WMA signals",
      html: welcomeHtml,
      headers: {
        "List-Unsubscribe": `<https://mungbeans.io/.netlify/functions/unsubscribe?email=${encodeURIComponent(subscriberEmail)}>`,
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
      },
    });
    console.log(`Welcome email sent to ${subscriberEmail}`);
  } catch (err) {
    console.error(`Failed to send welcome email: ${err}`);
  }
};
