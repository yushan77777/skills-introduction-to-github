/* Disk Usage app — list, drill-down, history trend and treemap.
   No external chart libraries: everything is hand-rendered SVG so the
   site works on an internal server with no internet access. */

(function () {
  "use strict";

  var API_DIRS = "/disk-usage/api/directories/";
  var API_HIST = "/disk-usage/api/history/";
  var SVG_NS = "http://www.w3.org/2000/svg";

  // Sequential blue ramp (light -> dark = small -> large). Ink color is
  // chosen per step so in-cell labels always clear contrast.
  var SEQ = [
    { fill: "var(--seq-1)", ink: "#0b0b0b" },
    { fill: "var(--seq-2)", ink: "#0b0b0b" },
    { fill: "var(--seq-3)", ink: "#0b0b0b" },
    { fill: "var(--seq-4)", ink: "#ffffff" },
    { fill: "var(--seq-5)", ink: "#ffffff" },
    { fill: "var(--seq-6)", ink: "#ffffff" },
    { fill: "var(--seq-7)", ink: "#ffffff" },
  ];

  var state = {
    parent: "",         // "" = top level, otherwise the drilled-into path
    minDepth: null,     // depth of the top-level rows (captured on first load)
    rows: [],           // rows at the current level
    selected: null,     // selected row object
    history: null,      // {directory, points} for the chart
    treemapParent: null // directory currently shown in the treemap modal
  };

  var el = {
    layout: document.querySelector(".du-layout"),
    breadcrumb: document.getElementById("du-breadcrumb"),
    rows: document.getElementById("du-rows"),
    error: document.getElementById("du-error"),
    scanTime: document.getElementById("du-scan-time"),
    totalSize: document.getElementById("du-total-size"),
    historyTitle: document.getElementById("du-history-title"),
    historyBody: document.getElementById("du-history-body"),
    treemapBtn: document.getElementById("du-treemap-btn"),
    modal: document.getElementById("du-modal"),
    modalClose: document.getElementById("du-modal-close"),
    modalTitle: document.getElementById("du-modal-title"),
    modalSub: document.getElementById("du-modal-sub"),
    treemap: document.getElementById("du-treemap"),
    tooltip: document.getElementById("du-tooltip"),
  };
  if (!el.layout) return; // not on the disk-usage page

  var HIST_TABLE = el.layout.dataset.histTable;

  /* ---------------- helpers ---------------- */

  function fetchJSON(url) {
    return fetch(url).then(function (resp) {
      return resp.json().catch(function () {
        throw new Error("HTTP " + resp.status + " from " + url);
      }).then(function (data) {
        if (!resp.ok) throw new Error(data.error || "HTTP " + resp.status);
        return data;
      });
    });
  }

  function fmtSize(kb) {
    var units = ["KB", "MB", "GB", "TB", "PB"];
    var v = kb, i = 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    var digits = v >= 100 || i === 0 ? 0 : v >= 10 ? 1 : 2;
    return v.toFixed(digits) + " " + units[i];
  }

  function parseTs(s) {
    return s ? new Date(String(s).replace(" ", "T")) : null;
  }

  function pad(n) { return String(n).padStart(2, "0"); }

  function fmtTs(s, withSeconds) {
    var d = parseTs(s);
    if (!d || isNaN(d)) return "–";
    var out = d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
      " " + pad(d.getHours()) + ":" + pad(d.getMinutes());
    if (withSeconds) out += ":" + pad(d.getSeconds());
    return out;
  }

  function baseName(path) {
    if (path === "/") return "/";
    var parts = path.replace(/\/+$/, "").split("/");
    return parts[parts.length - 1] || path;
  }

  function showError(msg) {
    el.error.textContent = msg;
    el.error.hidden = false;
  }
  function clearError() { el.error.hidden = true; }

  function svgEl(tag, attrs) {
    var node = document.createElementNS(SVG_NS, tag);
    for (var k in attrs) node.setAttribute(k, attrs[k]);
    return node;
  }

  /* ---------------- tooltip ---------------- */

  function tooltipShow(evt, lines) {
    // lines: [{cls, text}] — value first, label after
    el.tooltip.textContent = "";
    lines.forEach(function (l) {
      var d = document.createElement("div");
      d.className = l.cls;
      d.textContent = l.text; // untrusted data -> textContent, never innerHTML
      el.tooltip.appendChild(d);
    });
    el.tooltip.hidden = false;
    var pad = 14;
    var r = el.tooltip.getBoundingClientRect();
    var x = evt.clientX + pad, y = evt.clientY + pad;
    if (x + r.width > window.innerWidth - 8) x = evt.clientX - r.width - pad;
    if (y + r.height > window.innerHeight - 8) y = evt.clientY - r.height - pad;
    el.tooltip.style.left = x + "px";
    el.tooltip.style.top = y + "px";
  }
  function tooltipHide() { el.tooltip.hidden = true; }

  /* ---------------- breadcrumb ---------------- */

  // The breadcrumb shows the real path of the current level, one segment per
  // crumb, so reading it left to right gives the full directory path.
  // Segments above the scan's minimum depth aren't queryable on their own,
  // so they are collapsed into one leading crumb (e.g. "/data") that
  // navigates back to the top level.
  function renderBreadcrumb() {
    el.breadcrumb.textContent = "";
    var crumbs = [];
    if (!state.parent) {
      crumbs.push({ label: "/", target: null, title: "Top level" });
    } else {
      var segs = state.parent.replace(/^\/+|\/+$/g, "").split("/");
      var prefixN = Math.min(
        Math.max((state.minDepth || 1) - 1, 0), segs.length - 1
      );
      crumbs.push({
        label: prefixN > 0 ? "/" + segs.slice(0, prefixN).join("/") : "/",
        target: "",
        title: "Back to top level"
      });
      for (var i = prefixN; i < segs.length; i++) {
        var path = "/" + segs.slice(0, i + 1).join("/");
        var last = i === segs.length - 1;
        crumbs.push({ label: segs[i], target: last ? null : path, title: path });
      }
    }
    crumbs.forEach(function (c, i) {
      if (i > 0) {
        var sep = document.createElement("span");
        sep.className = "du-crumb-sep";
        sep.textContent = "›";
        el.breadcrumb.appendChild(sep);
      }
      if (c.target === null) {
        var cur = document.createElement("span");
        cur.className = "du-crumb-current";
        cur.textContent = c.label;
        cur.title = c.title;
        el.breadcrumb.appendChild(cur);
      } else {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.textContent = c.label;
        btn.title = c.title;
        btn.addEventListener("click", function () { loadLevel(c.target); });
        el.breadcrumb.appendChild(btn);
      }
    });
  }

  /* ---------------- directory list ---------------- */

  function folderIcon() {
    var svg = svgEl("svg", { viewBox: "0 0 24 24", width: 16, height: 16, "aria-hidden": "true" });
    svg.appendChild(svgEl("path", {
      d: "M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z",
      fill: "currentColor", opacity: "0.9"
    }));
    return svg;
  }

  function renderRows() {
    el.rows.textContent = "";
    if (!state.rows.length) {
      var empty = document.createElement("div");
      empty.className = "du-empty";
      empty.textContent = "No sub-directories recorded at this level.";
      el.rows.appendChild(empty);
      return;
    }
    var maxKb = Math.max.apply(null, state.rows.map(function (r) { return r.size_kb; })) || 1;
    var totalKb = state.rows.reduce(function (s, r) { return s + r.size_kb; }, 0) || 1;

    state.rows.forEach(function (row) {
      var div = document.createElement("div");
      div.className = "du-row";
      div.setAttribute("role", "option");
      div.tabIndex = 0;

      // directory (icon + basename + full path)
      var dir = document.createElement("div");
      dir.className = "du-row-dir";
      dir.appendChild(folderIcon());
      var nameWrap = document.createElement("div");
      nameWrap.style.minWidth = "0";
      var name = document.createElement("div");
      name.className = "du-dirname";
      name.textContent = baseName(row.directory);
      var path = document.createElement("div");
      path.className = "du-dirpath";
      path.textContent = row.directory;
      nameWrap.appendChild(name);
      nameWrap.appendChild(path);
      dir.appendChild(nameWrap);

      // proportional bar + share of level
      var barCell = document.createElement("div");
      barCell.style.display = "flex";
      barCell.style.alignItems = "center";
      var track = document.createElement("div");
      track.className = "du-bar-track";
      track.style.flex = "1";
      var bar = document.createElement("div");
      bar.className = "du-bar";
      bar.style.width = Math.max(1.5, (row.size_kb / maxKb) * 100) + "%";
      track.appendChild(bar);
      var pct = document.createElement("span");
      pct.className = "du-row-pct";
      pct.textContent = ((row.size_kb / totalKb) * 100).toFixed(1) + "%";
      barCell.appendChild(track);
      barCell.appendChild(pct);

      var size = document.createElement("div");
      size.className = "du-row-size";
      size.textContent = fmtSize(row.size_kb);

      var time = document.createElement("div");
      time.className = "du-row-time";
      time.textContent = fmtTs(row.scan_start_time);
      time.title = "Scan: " + fmtTs(row.scan_start_time, true) +
        " → " + fmtTs(row.scan_end_time, true);

      div.appendChild(dir);
      div.appendChild(barCell);
      div.appendChild(size);
      div.appendChild(time);

      div.addEventListener("click", function () { selectRow(row, div); });
      div.addEventListener("dblclick", function () { drillDown(row); });
      div.addEventListener("keydown", function (e) {
        if (e.key === "Enter") drillDown(row);
        if (e.key === " ") { e.preventDefault(); selectRow(row, div); }
      });

      row._el = div;
      el.rows.appendChild(div);
    });
  }

  function selectRow(row, div) {
    state.selected = row;
    el.rows.querySelectorAll(".du-row.selected").forEach(function (n) {
      n.classList.remove("selected");
    });
    if (div) div.classList.add("selected");
    loadHistory(row.directory);
  }

  function drillDown(row) {
    loadLevel(row.directory);
  }

  function loadLevel(parent) {
    clearError();
    el.rows.textContent = "";
    var loading = document.createElement("div");
    loading.className = "du-loading";
    loading.textContent = "Loading…";
    el.rows.appendChild(loading);

    var url = parent ? API_DIRS + "?parent=" + encodeURIComponent(parent) : API_DIRS;
    return fetchJSON(url).then(function (data) {
      state.rows = data.rows;
      state.parent = parent || "";
      if (!parent && data.rows.length) {
        state.minDepth = data.rows[0].depth;
      }
      renderBreadcrumb();
      renderRows();

      updateBadges(data);

      // Auto-select the largest directory so the trend shows immediately.
      if (state.rows.length) {
        selectRow(state.rows[0], state.rows[0]._el);
      } else if (parent) {
        // no children: keep showing the parent's own history
        loadHistory(parent);
      }
    }).catch(function (err) {
      el.rows.textContent = "";
      showError("Could not load directories: " + err.message);
    });
  }

  function updateBadges(data) {
    el.scanTime.textContent = data.scan_start_time
      ? fmtTs(data.scan_start_time) + " → " + fmtTs(data.scan_end_time)
      : "no scan data";
    el.totalSize.textContent = "Total " + fmtSize(data.total_kb);
  }

  // Silent refresh: re-fetch the current level (and open charts) without
  // loading placeholders, keeping selection, scroll position and breadcrumb.
  function refreshLevel() {
    var parent = currentParent();
    var url = parent ? API_DIRS + "?parent=" + encodeURIComponent(parent) : API_DIRS;
    fetchJSON(url).then(function (data) {
      if (parent !== currentParent()) return; // user navigated meanwhile
      var selectedDir = state.selected && state.selected.directory;
      var scrollTop = el.rows.scrollTop;
      state.rows = data.rows;
      renderRows();
      el.rows.scrollTop = scrollTop;
      updateBadges(data);
      clearError();

      var match = null;
      if (selectedDir) {
        state.rows.forEach(function (r) {
          if (r.directory === selectedDir) match = r;
        });
      }
      if (match) {
        // keep the user's selection; refresh its trend for new scan points
        state.selected = match;
        match._el.classList.add("selected");
        loadHistory(match.directory);
      } else if (state.rows.length) {
        selectRow(state.rows[0], state.rows[0]._el);
      } else if (parent) {
        loadHistory(parent);
      }

      if (!el.modal.hidden && state.treemapParent !== null) {
        openTreemap(state.treemapParent, true);
      }
    }).catch(function () {
      // keep showing the previous data; the next tick will retry
    });
  }

  /* ---------------- history line chart ---------------- */

  function loadHistory(directory) {
    var label = document.createElement("span");
    label.className = "du-dir-label";
    label.textContent = directory;
    el.historyTitle.textContent = "Usage history — ";
    el.historyTitle.appendChild(label);

    fetchJSON(API_HIST + "?directory=" + encodeURIComponent(directory))
      .then(function (data) {
        if (state.selected && data.directory !== state.selected.directory) return;
        state.history = data;
        drawHistory();
      })
      .catch(function (err) {
        el.historyBody.textContent = "";
        var d = document.createElement("div");
        d.className = "du-empty";
        d.textContent = "Could not load history: " + err.message;
        el.historyBody.appendChild(d);
      });
  }

  function niceStep(rough) {
    var mag = Math.pow(10, Math.floor(Math.log10(rough)));
    var norm = rough / mag;
    var step = norm >= 5 ? 10 : norm >= 2 ? 5 : norm >= 1 ? 2 : 1;
    return step * mag;
  }

  function drawHistory() {
    var data = state.history;
    el.historyBody.textContent = "";
    if (!data) return;

    var points = data.points.map(function (p) {
      return { t: parseTs(p.scan_start_time), kb: p.size_kb, raw: p.scan_start_time };
    }).filter(function (p) { return p.t && !isNaN(p.t); });

    if (!points.length) {
      var d = document.createElement("div");
      d.className = "du-empty";
      d.textContent = "No history recorded for this directory in " + HIST_TABLE + ".";
      el.historyBody.appendChild(d);
      return;
    }

    var box = el.historyBody.getBoundingClientRect();
    var W = Math.max(320, box.width), H = Math.max(140, box.height);
    var m = { top: 14, right: 26, bottom: 26, left: 70 };
    var iw = W - m.left - m.right, ih = H - m.top - m.bottom;

    var t0 = points[0].t.getTime(), t1 = points[points.length - 1].t.getTime();
    if (t0 === t1) { t0 -= 43200000; t1 += 43200000; } // single point: pad ±12 h
    var kbMax = Math.max.apply(null, points.map(function (p) { return p.kb; }));
    if (kbMax <= 0) kbMax = 1;
    // pick a display unit first so tick labels are clean numbers in that unit
    var units = ["KB", "MB", "GB", "TB", "PB"];
    var unitIdx = 0, unitDiv = 1, probe = kbMax;
    while (probe >= 1024 && unitIdx < units.length - 1) {
      probe /= 1024; unitDiv *= 1024; unitIdx++;
    }
    var stepU = niceStep((kbMax / unitDiv) / 3.5);
    var yMaxU = Math.ceil((kbMax / unitDiv) * 1.06 / stepU) * stepU;
    var yMax = yMaxU * unitDiv;

    function X(t) { return m.left + ((t - t0) / (t1 - t0)) * iw; }
    function Y(kb) { return m.top + ih - (kb / yMax) * ih; }

    var svg = svgEl("svg", { viewBox: "0 0 " + W + " " + H, preserveAspectRatio: "none" });
    svg.style.width = "100%";
    svg.style.height = "100%";

    // horizontal gridlines + y tick labels (clean numbers, human-readable)
    for (var u = 0; u <= yMaxU + 1e-9; u += stepU) {
      var gy = Y(u * unitDiv);
      svg.appendChild(svgEl("line", {
        x1: m.left, x2: W - m.right, y1: gy, y2: gy,
        stroke: u === 0 ? "var(--baseline)" : "var(--grid)", "stroke-width": 1
      }));
      var lab = svgEl("text", {
        x: m.left - 9, y: gy + 3.5, "text-anchor": "end",
        "font-size": 10.5, fill: "var(--text-muted)"
      });
      lab.textContent = (u % 1 === 0 ? u.toLocaleString() : u.toFixed(1)) +
        " " + units[unitIdx];
      svg.appendChild(lab);
    }

    // x tick labels (~4, evenly spaced)
    var nx = Math.min(4, points.length);
    for (var i = 0; i < nx; i++) {
      var tt = t0 + ((t1 - t0) * i) / Math.max(1, nx - 1);
      var xt = svgEl("text", {
        x: X(tt), y: H - 7,
        "text-anchor": i === 0 ? "start" : i === nx - 1 ? "end" : "middle",
        "font-size": 10.5, fill: "var(--text-muted)"
      });
      var td = new Date(tt);
      xt.textContent = td.getFullYear() + "-" + pad(td.getMonth() + 1) + "-" +
        pad(td.getDate()) + " " + pad(td.getHours()) + ":" + pad(td.getMinutes());
      svg.appendChild(xt);
    }

    // area wash (~10% opacity) + 2px line
    var lineD = points.map(function (p, idx) {
      return (idx ? "L" : "M") + X(p.t.getTime()).toFixed(1) + " " + Y(p.kb).toFixed(1);
    }).join(" ");
    if (points.length > 1) {
      var areaD = lineD +
        " L" + X(t1).toFixed(1) + " " + Y(0).toFixed(1) +
        " L" + X(points[0].t.getTime()).toFixed(1) + " " + Y(0).toFixed(1) + " Z";
      svg.appendChild(svgEl("path", { d: areaD, fill: "var(--accent)", opacity: 0.1 }));
      svg.appendChild(svgEl("path", {
        d: lineD, fill: "none", stroke: "var(--accent)",
        "stroke-width": 2, "stroke-linecap": "round", "stroke-linejoin": "round"
      }));
    }

    // end dot with a surface ring so it reads over the line
    var last = points[points.length - 1];
    svg.appendChild(svgEl("circle", {
      cx: X(last.t.getTime()), cy: Y(last.kb), r: 4.5,
      fill: "var(--accent)", stroke: "var(--surface)", "stroke-width": 2
    }));

    // crosshair + hover dot (hidden until pointermove)
    var cross = svgEl("line", {
      y1: m.top, y2: m.top + ih, stroke: "var(--baseline)",
      "stroke-width": 1, visibility: "hidden"
    });
    var hoverDot = svgEl("circle", {
      r: 4.5, fill: "var(--accent)", stroke: "var(--surface)",
      "stroke-width": 2, visibility: "hidden"
    });
    svg.appendChild(cross);
    svg.appendChild(hoverDot);

    // transparent hit layer — the whole plot is the target, the crosshair
    // snaps to the nearest scan so nobody has to aim at a 2px line
    var hit = svgEl("rect", {
      x: m.left, y: m.top, width: iw, height: ih, fill: "transparent"
    });
    hit.style.cursor = "crosshair";
    hit.addEventListener("pointermove", function (evt) {
      var rect = svg.getBoundingClientRect();
      var px = ((evt.clientX - rect.left) / rect.width) * W;
      var best = points[0], bd = Infinity;
      points.forEach(function (p) {
        var d = Math.abs(X(p.t.getTime()) - px);
        if (d < bd) { bd = d; best = p; }
      });
      var bx = X(best.t.getTime()), by = Y(best.kb);
      cross.setAttribute("x1", bx); cross.setAttribute("x2", bx);
      cross.setAttribute("visibility", "visible");
      hoverDot.setAttribute("cx", bx); hoverDot.setAttribute("cy", by);
      hoverDot.setAttribute("visibility", "visible");
      tooltipShow(evt, [
        { cls: "tt-value", text: fmtSize(best.kb) },
        { cls: "tt-sub", text: "scanned " + fmtTs(best.raw, true) },
      ]);
    });
    hit.addEventListener("pointerleave", function () {
      cross.setAttribute("visibility", "hidden");
      hoverDot.setAttribute("visibility", "hidden");
      tooltipHide();
    });
    svg.appendChild(hit);

    el.historyBody.appendChild(svg);
  }

  /* ---------------- treemap ---------------- */

  function worstRatio(row, sum, side) {
    var thickness = sum / side, worst = 0;
    row.forEach(function (v) {
      var len = v / thickness;
      var r = Math.max(thickness / len, len / thickness);
      if (r > worst) worst = r;
    });
    return worst;
  }

  // Squarified treemap: returns rects in the same order as `values`.
  function squarify(values, x, y, w, h) {
    var rects = [], i = 0, n = values.length;
    while (i < n && w > 0.5 && h > 0.5) {
      var horizontal = w >= h;
      var side = horizontal ? h : w;
      var row = [values[i]], rowSum = values[i], j = i + 1;
      var worst = worstRatio(row, rowSum, side);
      while (j < n) {
        var cand = row.concat(values[j]);
        var candSum = rowSum + values[j];
        var candWorst = worstRatio(cand, candSum, side);
        if (candWorst > worst) break;
        row = cand; rowSum = candSum; worst = candWorst; j++;
      }
      var thickness = rowSum / side, offset = 0;
      row.forEach(function (v) {
        var len = v / thickness;
        rects.push(horizontal
          ? { x: x, y: y + offset, w: thickness, h: len }
          : { x: x + offset, y: y, w: len, h: thickness });
        offset += len;
      });
      if (horizontal) { x += thickness; w -= thickness; }
      else { y += thickness; h -= thickness; }
      i = j;
    }
    // degenerate leftover space: stack remaining items as slivers
    for (; i < n; i++) rects.push({ x: x, y: y, w: Math.max(w, 0), h: Math.max(h, 0) });
    return rects;
  }

  function seqStep(ratio) {
    // darker = larger share of the parent
    if (ratio > 0.62) return SEQ[6];
    if (ratio > 0.42) return SEQ[5];
    if (ratio > 0.27) return SEQ[4];
    if (ratio > 0.15) return SEQ[3];
    if (ratio > 0.07) return SEQ[2];
    if (ratio > 0.02) return SEQ[1];
    return SEQ[0];
  }

  function openTreemap(directory, silent) {
    state.treemapParent = directory;
    el.modal.hidden = false;
    el.modalTitle.textContent = "Treemap — folder sizes";
    el.modalSub.textContent = directory || "Top level";
    if (!silent) {
      // silent refresh keeps the previous render until new data arrives
      el.treemap.textContent = "";
      var loading = document.createElement("div");
      loading.className = "du-loading";
      loading.textContent = "Loading…";
      el.treemap.appendChild(loading);
    }

    var url = directory ? API_DIRS + "?parent=" + encodeURIComponent(directory) : API_DIRS;
    fetchJSON(url).then(function (data) {
      if (el.modal.hidden || state.treemapParent !== directory) return;
      drawTreemap(data.rows, directory);
    }).catch(function (err) {
      el.treemap.textContent = "";
      var d = document.createElement("div");
      d.className = "du-empty";
      d.textContent = "Could not load treemap: " + err.message;
      el.treemap.appendChild(d);
    });
  }

  function drawTreemap(rows, directory) {
    el.treemap.textContent = "";
    rows = rows.filter(function (r) { return r.size_kb > 0; });
    if (!rows.length) {
      var d = document.createElement("div");
      d.className = "du-empty";
      d.textContent = "No sub-directories with data inside " + (directory || "the top level") + ".";
      el.treemap.appendChild(d);
      return;
    }

    var box = el.treemap.getBoundingClientRect();
    var W = Math.max(400, box.width), H = Math.max(280, box.height);
    var total = rows.reduce(function (s, r) { return s + r.size_kb; }, 0);
    var maxKb = rows[0].size_kb; // rows arrive sorted desc
    var values = rows.map(function (r) { return (r.size_kb / total) * W * H; });
    var rects = squarify(values, 0, 0, W, H);

    var svg = svgEl("svg", { viewBox: "0 0 " + W + " " + H, preserveAspectRatio: "none" });
    svg.style.width = "100%";
    svg.style.height = "100%";

    rows.forEach(function (row, i) {
      var r = rects[i];
      if (!r || r.w < 1 || r.h < 1) return;
      var step = seqStep(row.size_kb / maxKb);
      var g = svgEl("g", { class: "du-cell" });
      g.setAttribute("tabindex", "0");

      // 1px inset on each side = the 2px surface gap between neighbours
      g.appendChild(svgEl("rect", {
        x: (r.x + 1).toFixed(1), y: (r.y + 1).toFixed(1),
        width: Math.max(0.5, r.w - 2).toFixed(1),
        height: Math.max(0.5, r.h - 2).toFixed(1),
        rx: 3, fill: step.fill
      }));

      // label only when it fits comfortably — never clipped
      var name = baseName(row.directory);
      var sizeTxt = fmtSize(row.size_kb);
      if (r.w > 74 && r.h > 40) {
        var maxChars = Math.floor((r.w - 18) / 7);
        var shown = name.length > maxChars ? name.slice(0, Math.max(1, maxChars - 1)) + "…" : name;
        var t1 = svgEl("text", {
          x: r.x + 9, y: r.y + 19, "font-size": 11.5,
          "font-weight": 600, fill: step.ink
        });
        t1.textContent = shown;
        g.appendChild(t1);
        var t2 = svgEl("text", {
          x: r.x + 9, y: r.y + 33, "font-size": 10.5,
          fill: step.ink, opacity: 0.82
        });
        t2.textContent = sizeTxt;
        g.appendChild(t2);
      }

      g.addEventListener("pointermove", function (evt) {
        tooltipShow(evt, [
          { cls: "tt-value", text: sizeTxt + "  ·  " + ((row.size_kb / total) * 100).toFixed(1) + "%" },
          { cls: "tt-label", text: row.directory },
          { cls: "tt-sub", text: "double-click to drill down" },
        ]);
      });
      g.addEventListener("pointerleave", tooltipHide);
      g.addEventListener("click", function () {
        // single click selects the folder + shows its history behind the modal
        var match = state.rows.find(function (x) { return x.directory === row.directory; });
        if (match) selectRow(match, match._el);
      });
      g.addEventListener("dblclick", function () {
        tooltipHide();
        loadLevel(row.directory);
        openTreemap(row.directory);
      });

      svg.appendChild(g);
    });

    el.treemap.appendChild(svg);
  }

  function closeTreemap() {
    el.modal.hidden = true;
    state.treemapParent = null;
    tooltipHide();
  }

  el.treemapBtn.addEventListener("click", function () {
    // treemap of the level currently shown in the list
    openTreemap(currentParent());
  });
  el.modalClose.addEventListener("click", closeTreemap);
  el.modal.addEventListener("click", function (evt) {
    if (evt.target === el.modal) closeTreemap();
  });
  document.addEventListener("keydown", function (evt) {
    if (evt.key === "Escape" && !el.modal.hidden) closeTreemap();
  });

  function currentParent() {
    return state.parent || "";
  }

  /* ---------------- resize / boot ---------------- */

  var resizeTimer = null;
  window.addEventListener("resize", function () {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function () {
      drawHistory();
      if (!el.modal.hidden && state.treemapParent !== null) {
        openTreemap(state.treemapParent);
      }
    }, 150);
  });

  /* ---------------- auto-refresh (every minute) ---------------- */

  var REFRESH_MS = 60000;
  var lastRefresh = Date.now();

  setInterval(function () {
    if (document.hidden) return; // don't poll while the tab is in background
    lastRefresh = Date.now();
    refreshLevel();
  }, REFRESH_MS);

  // catch up right away when the tab becomes visible again
  document.addEventListener("visibilitychange", function () {
    if (!document.hidden && Date.now() - lastRefresh > REFRESH_MS) {
      lastRefresh = Date.now();
      refreshLevel();
    }
  });

  loadLevel("");
})();
