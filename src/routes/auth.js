import express from "express";
import axios from "axios";

const router = express.Router();

router.get("/strava", (req, res) => {
  const scope = "read,activity:read_all";

  const authUrl =
    `https://www.strava.com/oauth/authorize` +
    `?client_id=${process.env.STRAVA_CLIENT_ID}` +
    `&response_type=code` +
    `&redirect_uri=${process.env.STRAVA_REDIRECT_URI}` +
    `&approval_prompt=force` +
    `&scope=${scope}`;

  res.redirect(authUrl);
});

router.get("/strava/callback", async (req, res) => {
  const { code } = req.query;

  if (!code) {
    console.warn("Strava callback missing ?code");
    return res.status(400).json({ error: "Missing authorization code" });
  }

  try {
    console.log("Received authorization code from Strava:", code);
    const response = await axios.post("https://www.strava.com/oauth/token", {
      client_id: process.env.STRAVA_CLIENT_ID,
      client_secret: process.env.STRAVA_CLIENT_SECRET,
      code,
      grant_type: "authorization_code",
    });

    const { access_token, refresh_token, expires_at, athlete } = response.data;
    console.log("Strava authentication successful for athlete:", athlete.id);
    res.json({
      access_token,
      refresh_token,
      expires_at,
      athlete,
    });
  } catch (error) {
    console.error("Strava token exchange failed:", {
      message: error?.message,
      status: error?.response?.status,
      data: error?.response?.data,
    });
    res.status(500).json({
      error: "Strava authentication failed",
      details: error.response?.data || error.message,
    });
  }
});

export default router;
