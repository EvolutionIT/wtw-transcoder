import { Router } from "express";
import crypto from "crypto";
import {
  loginLimiter,
  bruteForceProtection,
  recordFailedAttempt,
  clearFailedAttempts,
} from "../middleware/auth.js";

const router = Router();

router.get("/login", (req, res) => {
  if (req.session && req.session.authenticated) {
    return res.redirect("/");
  }

  res.render("login", {
    title: "Login - Video Transcoding Service",
    error: null,
    returnTo: req.session.returnTo || "/",
  });
});

router.post("/login", loginLimiter, bruteForceProtection, async (req, res) => {
  const { password, returnTo } = req.body;
  const ip = req.ip || req.connection.remoteAddress;

  if (!password) {
    return res.render("login", {
      title: "Login - Video Transcoding Service",
      error: "Password is required",
      returnTo: returnTo || "/",
    });
  }

  const correctPassword = process.env.DASHBOARD_PASSWORD;

  if (!correctPassword) {
    console.error("DASHBOARD_PASSWORD environment variable not set");
    return res.render("login", {
      title: "Login - Video Transcoding Service",
      error: "Authentication not configured",
      returnTo: returnTo || "/",
    });
  }

  const providedHash = crypto
    .createHash("sha256")
    .update(password)
    .digest("hex");
  const correctHash = crypto
    .createHash("sha256")
    .update(correctPassword)
    .digest("hex");

  const isValid = crypto.timingSafeEqual(
    Buffer.from(providedHash, "hex"),
    Buffer.from(correctHash, "hex"),
  );

  if (isValid) {
    clearFailedAttempts(ip);

    req.session.authenticated = true;
    req.session.loginTime = new Date().toISOString();

    console.log(`Successful login from ${ip} at ${new Date().toISOString()}`);

    const redirectTo = returnTo || req.session.returnTo || "/";
    delete req.session.returnTo;

    res.redirect(redirectTo);
  } else {
    recordFailedAttempt(ip);

    console.warn(
      `Failed login attempt from ${ip} at ${new Date().toISOString()}`,
    );

    res.render("login", {
      title: "Login - Video Transcoding Service",
      error: "Invalid password",
      returnTo: returnTo || "/",
    });
  }
});

router.post("/logout", (req, res) => {
  const ip = req.ip || req.connection.remoteAddress;

  if (req.session) {
    console.log(`User logged out from ${ip} at ${new Date().toISOString()}`);
    req.session.destroy((err) => {
      if (err) {
        console.error("Error destroying session:", err);
      }
      res.redirect("/login");
    });
  } else {
    res.redirect("/login");
  }
});

router.get("/auth/status", (req, res) => {
  if (req.session && req.session.authenticated) {
    res.json({
      authenticated: true,
      loginTime: req.session.loginTime,
      sessionId: req.sessionID,
    });
  } else {
    res.json({
      authenticated: false,
    });
  }
});

export default router;
