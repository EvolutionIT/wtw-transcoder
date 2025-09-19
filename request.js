const getRes = async () => {
  const videoKey = "uploads/1757924783184.mp4";

  const response = await fetch("http://localhost:3000/api/transcode", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": "secret",
    },
    body: JSON.stringify({
      key: videoKey,
      resolutions: ["720p", "480p", "360p"],
    }),
  });

  const result = await response.json();

  console.log(result);
};

getRes();
