import rateLimit from "express-rate-limit";

const failedAttempts = new Map();

const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 5,
  message: {
    error: "Too many login attempts, please try again in 15 minutes",
  },
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => {
    return req.session && req.session.authenticated;
  },
});

const getDelayForAttempts = (attempts) => {
  if (attempts <= 2) return 0;
  if (attempts <= 4) return 1000;
  if (attempts <= 6) return 5000;
  return 10000;
};

const requireAuth = (req, res, next) => {
  if (req.session && req.session.authenticated) {
    return next();
  }

  req.session.returnTo = req.originalUrl;

  return res.redirect("/login");
};

const bruteForceProtection = (req, res, next) => {
  const ip = req.ip || req.connection.remoteAddress;
  const attempts = failedAttempts.get(ip) || { count: 0, lastAttempt: null };

  const now = Date.now();
  const timeSinceLastAttempt = now - (attempts.lastAttempt || 0);

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

const recordFailedAttempt = (ip) => {
  const attempts = failedAttempts.get(ip) || { count: 0, lastAttempt: null };
  attempts.count += 1;
  attempts.lastAttempt = Date.now();
  failedAttempts.set(ip, attempts);

  console.warn(
    `Failed login attempt from ${ip}. Total attempts: ${attempts.count}`,
  );
};

const clearFailedAttempts = (ip) => {
  failedAttempts.delete(ip);
};

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
);

export {
  requireAuth,
  loginLimiter,
  bruteForceProtection,
  recordFailedAttempt,
  clearFailedAttempts,
};
