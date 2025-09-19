import dotenv from "dotenv";
dotenv.config();

import express, { json, urlencoded, static as static_ } from "express";
import { join } from "path";
import cors from "cors";

import { initializeDatabase } from "./services/database.js";
import { initializeQueue } from "./services/queue.js";
import transcodingRoutes from "./routes/transcoding.js";
import dashboardRoutes from "./routes/dashboard.js";

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(json({ limit: "50mb" }));
app.use(urlencoded({ extended: true }));

app.use("/static", static_(join(process.cwd(), "public")));

app.set("views", join(process.cwd(), "src/views"));
app.set("view engine", "ejs");

app.use("/", dashboardRoutes);
app.use("/api", transcodingRoutes);

app.get("/health", (_req, res) => {
  res.json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
});

app.use((err, _req, res, next) => {
  console.error("Error:", err);
  res.status(500).json({
    error: "Internal server error",
    message: err.message,
  });

  next();
});

app.use((_req, res) => {
  res.status(404).json({ error: "Route not found" });
});

async function startServer() {
  try {
    await initializeDatabase();

    await initializeQueue();

    app.listen(PORT, () => {
      console.log(`Transcoding service running on port ${PORT}`);
      console.log(`Dashboard: http://localhost:${PORT}`);
      console.log(`API: http://localhost:${PORT}/api`);
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

process.on("SIGTERM", async () => {
  console.log("Received SIGTERM, shutting down gracefully...");
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("Received SIGINT, shutting down gracefully...");
  process.exit(0);
});

startServer();
