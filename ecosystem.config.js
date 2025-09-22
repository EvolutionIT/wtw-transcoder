module.exports = {
  apps: [
    {
      name: "transcoder",
      script: "src/app.js",
      cwd: "/var/www/transcoder",
      instances: 1,
      exec_mode: "fork",

      // Auto restart
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",

      // Environment variables
      env: {
        NODE_ENV: "production",
        PORT: 3000,
      },

      // Logging
      log_file: "/var/log/transcoder/combined.log",
      out_file: "/var/log/transcoder/out.log",
      error_file: "/var/log/transcoder/error.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",

      // Performance
      node_args: "--max-old-space-size=1024",

      // Graceful shutdown
      kill_timeout: 5000,
      listen_timeout: 10000,

      // Health monitoring
      min_uptime: "10s",
      max_restarts: 5,

      // Advanced settings
      merge_logs: true,
      combine_logs: true,
    },
  ],
};
