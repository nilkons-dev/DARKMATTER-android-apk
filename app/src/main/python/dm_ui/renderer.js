// Hidden page (runs in a background WebView): turns the engine's chart SVGs into PNG pictures.
(function () {
  "use strict";
  var token = new URLSearchParams(location.search).get("t") || "";
  var H = { "X-Token": token };
  function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

  function render(job) {
    return new Promise(function (resolve, reject) {
      var done = false;
      var timer = setTimeout(function () { if (!done) { done = true; reject(new Error("SVG decode timeout")); } }, 30000);
      var img = new Image();
      img.onload = function () {
        if (done) return;
        done = true; clearTimeout(timer);
        try {
          var c = document.createElement("canvas");
          c.width = job.w; c.height = job.h;
          var ctx = c.getContext("2d");
          ctx.drawImage(img, 0, 0, job.w, job.h);
          var url = c.toDataURL("image/png");
          resolve(url.slice(url.indexOf(",") + 1));
        } catch (e) { reject(e); }
      };
      img.onerror = function () { if (!done) { done = true; clearTimeout(timer); reject(new Error("SVG could not be decoded")); } };
      img.src = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(job.svg);
    });
  }

  async function post(body) {
    await fetch("/api/render/done", {
      method: "POST",
      headers: { "X-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
  }

  async function loop() {
    for (;;) {
      try {
        var r = await fetch("/api/render/next", { headers: H });
        if (r.status === 204) { await sleep(50); continue; }
        if (!r.ok) { await sleep(2000); continue; }
        var job = await r.json();
        var out;
        try { out = { id: job.id, png: await render(job) }; }
        catch (e) { out = { id: job.id, error: String((e && e.message) || e) }; }
        await post(out);
      } catch (e) { await sleep(1500); }
    }
  }
  loop();
})();
