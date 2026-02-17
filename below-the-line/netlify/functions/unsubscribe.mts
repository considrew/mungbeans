/**
 * Unsubscribe function.
 * Stores unsubscribed emails in Netlify Blobs.
 * The weekly email script checks this list before sending.
 */
import { getStore } from "@netlify/blobs";
import type { Config } from "@netlify/functions";

export default async (req: Request) => {
  const url = new URL(req.url);
  const email = url.searchParams.get("email");

  if (!email) {
    return new Response(errorPage("No email provided."), {
      status: 400,
      headers: { "Content-Type": "text/html" },
    });
  }

  const store = getStore("unsubscribes");

  // Store the unsubscribe (key = email, value = timestamp)
  await store.set(email.toLowerCase().trim(), new Date().toISOString());

  return new Response(successPage(email), {
    status: 200,
    headers: { "Content-Type": "text/html" },
  });
};

export const config: Config = {
  path: "/.netlify/functions/unsubscribe",
};

function successPage(email: string): string {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Unsubscribed — mungbeans.io</title>
  <style>
    body { background: #0f0f1a; color: #e0e0e0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }
    .box { text-align: center; max-width: 400px; padding: 40px; }
    h1 { color: #e2b714; font-family: monospace; font-size: 32px; }
    p { line-height: 1.6; color: #aaa; }
    a { color: #e2b714; }
  </style>
</head>
<body>
  <div class="box">
    <h1>m</h1>
    <p>You've been unsubscribed.</p>
    <p style="font-size:14px; color:#666;">You won't receive any more emails from mungbeans.io.</p>
    <p><a href="https://mungbeans.io">← Back to mungbeans.io</a></p>
  </div>
</body>
</html>`;
}

function errorPage(message: string): string {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Error — mungbeans.io</title>
  <style>
    body { background: #0f0f1a; color: #e0e0e0; font-family: sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }
    .box { text-align: center; max-width: 400px; padding: 40px; }
    a { color: #e2b714; }
  </style>
</head>
<body>
  <div class="box">
    <p>${message}</p>
    <p><a href="https://mungbeans.io">← Back to mungbeans.io</a></p>
  </div>
</body>
</html>`;
}
