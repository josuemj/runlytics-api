import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import authRoutes from "./routes/auth.js";

const dotenvResult = dotenv.config();
if (dotenvResult.error) {
  console.error("Failed to load .env:", dotenvResult.error);
}

const app = express();

app.use(cors());
app.use(express.json());

app.use("/auth", authRoutes);

app.get("/", (req, res) => {
  res.send("Runlytics API running");
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  const nodeEnv = process.env.NODE_ENV || "development";
  const stravaClientIdSet = Boolean(process.env.STRAVA_CLIENT_ID);
  const stravaRedirectUri = process.env.STRAVA_REDIRECT_URI || "(missing)";

  console.log(`API running on port ${PORT} (NODE_ENV=${nodeEnv})`);
  console.log(
    `Config: STRAVA_CLIENT_ID=${
      stravaClientIdSet ? "(set)" : "(missing)"
    } STRAVA_REDIRECT_URI=${stravaRedirectUri}`
  );
});

app.use((req, res) => {
  res.status(404).json({ error: "Not found" });
});

app.use((err, req, res, next) => {
  console.error("Request error:", {
    method: req.method,
    path: req.originalUrl || req.url,
    message: err?.message,
    stack: err?.stack,
  });
  if (res.headersSent) return next(err);
  res.status(500).json({ error: "Internal server error" });
});
