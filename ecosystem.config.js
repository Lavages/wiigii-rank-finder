module.exports = {
  apps: [
    {
      name: "wiigii-hub",
      script: "./app.py",
      interpreter: "python",
    },
    {
      name: "wiigii-tunnel",
      script: "./cloudflared.exe",
      args: "tunnel --url http://127.0.0.1:5000",
      interpreter: "none",
    },
  ],
};
