import rateLimit from "express-rate-limit";

// In-memory store for failed login attempts (use Redis in production for multiple instances)
const failedAttempts = new Map();

// Rate limiter for login attempts
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 5, // Limit each IP to 5 requests per windowMs
  message: {
    error: "Too many login attempts, please try again in 15 minutes",
  },
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => {
    // Skip rate limiting if already authenticated
    return req.session && req.session.authenticated;
  },
});

// Progressive delay based on failed attempts
const getDelayForAttempts = (attempts) => {
  if (attempts <= 2) return 0;
  if (attempts <= 4) return 1000; // 1 second
  if (attempts <= 6) return 5000; // 5 seconds
  return 10000; // 10 seconds
};

// Middleware to check authentication
const requireAuth = (req, res, next) => {
  // Debug logging (remove in production)
  if (process.env.NODE_ENV !== "production") {
    console.log("Auth check:", {
      hasSession: !!req.session,
      authenticated: req.session?.authenticated,
      sessionId: req.sessionID,
      path: req.path,
    });
  }

  // Check if session exists and user is authenticated
  if (req.session && req.session.authenticated === true) {
    return next();
  }

  // Store the originally requested URL (only if session exists)
  if (req.session) {
    req.session.returnTo = req.originalUrl;
  }

  // Redirect to login page
  return res.redirect("/login");
};

// Middleware to add progressive delays for brute force protection
const bruteForceProtection = (req, res, next) => {
  const ip = req.ip || req.connection.remoteAddress;
  const attempts = failedAttempts.get(ip) || { count: 0, lastAttempt: null };

  const now = Date.now();
  const timeSinceLastAttempt = now - (attempts.lastAttempt || 0);

  // Reset counter if more than 1 hour has passed
  if (timeSinceLastAttempt > 60 * 60 * 1000) {
    failedAttempts.delete(ip);
    return next();
  }

  const requiredDelay = getDelayForAttempts(attempts.count);

  if (timeSinceLastAttempt < requiredDelay) {
    const remainingDelay = Math.ceil(
      (requiredDelay - timeSinceLastAttempt) / 1000,
    );
    return res.status(429).json({
      error: `Please wait ${remainingDelay} seconds before trying again`,
    });
  }

  next();
};

// Function to record failed attempt
const recordFailedAttempt = (ip) => {
  const attempts = failedAttempts.get(ip) || { count: 0, lastAttempt: null };
  attempts.count += 1;
  attempts.lastAttempt = Date.now();
  failedAttempts.set(ip, attempts);

  console.warn(
    `Failed login attempt from ${ip}. Total attempts: ${attempts.count}`,
  );
};

// Function to clear failed attempts on successful login
const clearFailedAttempts = (ip) => {
  failedAttempts.delete(ip);
};

// Clean up old entries periodically
setInterval(
  () => {
    const now = Date.now();
    const oneHourAgo = now - 60 * 60 * 1000;

    for (const [ip, attempts] of failedAttempts.entries()) {
      if (attempts.lastAttempt < oneHourAgo) {
        failedAttempts.delete(ip);
      }
    }
  },
  5 * 60 * 1000,
); // Clean up every 5 minutes

export {
  requireAuth,
  loginLimiter,
  bruteForceProtection,
  recordFailedAttempt,
  clearFailedAttempts,
};
