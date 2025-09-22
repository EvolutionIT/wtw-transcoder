import express from "express";
import session from "express-session";
import { join } from "path";
import { static as static_ } from "express";

// Import middleware
import { requireAuth } from "./middleware/auth.js";

// Import routes
import transcodingRoutes from "./routes/transcoding.js";
import dashboardRoutes from "./routes/dashboard.js";
import authRoutes from "./routes/auth.js";

// Import services
import { initializeDatabase } from "./services/database.js";
import { initializeQueue } from "./services/queue.js";

import dotenv from "dotenv";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

app.set("trust proxy", 1);

app.use(
  session({
    secret:
      process.env.SESSION_SECRET ||
      "your-super-secret-session-key-change-this-in-production",
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure:
        process.env.NODE_ENV === "production" && process.env.HTTPS === "true",
      httpOnly: true,
      maxAge: 24 * 60 * 60 * 1000,
    },
    name: "transcoder.sid",
  }),
);

app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

app.use("/static", static_(join(process.cwd(), "public")));

app.set("views", join(process.cwd(), "src/views"));
app.set("view engine", "ejs");

app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-XSS-Protection", "1; mode=block");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");

  if (req.path.startsWith("/") && !req.path.startsWith("/static")) {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
  }

  next();
});

const publicRoutes = ["/login", "/auth/status", "/auth/clear", "/health"];
const apiRoutes = ["/api"]; // API routes use different auth

app.use((req, res, next) => {
  if (publicRoutes.some((route) => req.path === route)) {
    return next();
  }

  if (apiRoutes.some((route) => req.path.startsWith(route))) {
    return next();
  }

  if (req.path.startsWith("/static")) {
    return next();
  }

  return requireAuth(req, res, next);
});

app.use("/", authRoutes);
app.use("/api/transcode", transcodingRoutes);

app.get("/health", (req, res) => {
  res.json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
});

app.use("/", dashboardRoutes);

app.use((req, res) => {
  res.status(404).render("error", {
    title: "Page Not Found",
    error: "The page you requested could not be found.",
    stack: null,
  });
});

app.use((err, req, res, next) => {
  console.error("Unhandled error:", err);

  res.status(err.status || 500).render("error", {
    title: "Server Error",
    error:
      process.env.NODE_ENV === "production"
        ? "An internal server error occurred"
        : err.message,
    stack: process.env.NODE_ENV === "production" ? null : err.stack,
  });
});

async function startServer() {
  try {
    if (!process.env.DASHBOARD_PASSWORD) {
      console.error("DASHBOARD_PASSWORD environment variable is required");
      process.exit(1);
    }

    if (!process.env.SESSION_SECRET && process.env.NODE_ENV === "production") {
      console.warn(
        "SESSION_SECRET not set. Using default (not recommended for production)",
      );
    }

    console.log("Starting Video Transcoding Service...");

    await initializeDatabase();

    await initializeQueue();

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);

      if (process.env.NODE_ENV !== "production") {
        console.log(`API: http://localhost:${PORT}/api/transcode`);
      }
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("Received SIGTERM, shutting down gracefully...");
  process.exit(0);
});

process.on("SIGINT", () => {
  console.log("Received SIGINT, shutting down gracefully...");
  process.exit(0);
});

startServer();

export default app;
