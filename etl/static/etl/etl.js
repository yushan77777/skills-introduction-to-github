/* ETL console front end.

   The form is generated from /etl/api/jobs/, which is generated from the jobs
   discovered in the ETL source. Adding an ETL to run_etl() adds it here with
   no change to this file.

   Nothing here decides whether an ETL may start: the Run button asks the
   server, and a 409 means the server said no. The disabled state below is a
   courtesy, not the guard.

   No external libraries — the platform runs on an internal server with no
   internet access. */

(function () {
  "use strict";

  var layout = document.querySelector(".etl-layout");
  if (!layout) return;                       // not on the ETL page

  var API = layout.dataset.api;
  var POLL_MS = 900;

  var state = {
    jobs: {},          // key -> job definition
    current: null,     // selected job
    configKeys: { oracle: [], greenplum: [], other: [] },
    defaults: {},
    run: null,         // snapshot of the run being watched
    cursor: 0,         // log cursor for incremental fetches
    timer: null,
    busy: false        // an ETL is running somewhere on the server
  };

  /* ---------------- small helpers ---------------- */

  function $(id) { return document.getElementById(id); }

  function el(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text != null) node.textContent = text;
    return node;
  }

  function csrfToken() {
    var input = layout.querySelector("input[name=csrfmiddlewaretoken]");
    return input ? input.value : "";
  }

  function api(path, options) {
    options = options || {};
    var headers = { "Content-Type": "application/json" };
    if (options.method && options.method !== "GET") {
      headers["X-CSRFToken"] = csrfToken();
    }
    return fetch(API + path, {
      method: options.method || "GET",
      headers: headers,
      body: options.body ? JSON.stringify(options.body) : undefined,
      credentials: "same-origin"
    }).then(function (resp) {
      return resp.json().catch(function () { return {}; }).then(function (body) {
        return { ok: resp.ok, status: resp.status, body: body };
      });
    });
  }

  function pad(n) { return String(n).padStart(2, "0"); }

  function fmtTime(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (isNaN(d)) return String(iso);
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
      " " + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
  }

  function fmtDuration(seconds) {
    if (seconds == null) return "—";
    var s = Math.max(0, Math.round(seconds));
    var h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
    if (h) return h + "h " + pad(m) + "m " + pad(r) + "s";
    if (m) return m + "m " + pad(r) + "s";
    return s + "s";
  }

  function banner(message, kind, items) {
    var box = $("etl-banner");
    box.className = "etl-banner" + (kind === "info" ? " etl-banner-info" : "");
    box.textContent = "";
    if (!message) { box.hidden = true; return; }
    box.appendChild(el("div", null, message));
    if (items && items.length) {
      var list = el("ul");
      items.forEach(function (t) { list.appendChild(el("li", null, t)); });
      box.appendChild(list);
    }
    box.hidden = false;
  }

  function formMessage(text, kind) {
    var node = $("etl-form-message");
    node.className = "etl-form-message" + (kind ? " is-" + kind : "");
    node.textContent = text || "";
  }

  /* ---------------- boot ---------------- */

  function boot() {
    $("etl-job").addEventListener("change", onSelectJob);
    $("etl-run-btn").addEventListener("click", onRun);
    $("etl-validate-btn").addEventListener("click", onValidate);
    $("etl-reset-btn").addEventListener("click", onReset);
    $("etl-stop-btn").addEventListener("click", onStop);
    $("etl-config-btn").addEventListener("click", openConfig);
    $("etl-config-close").addEventListener("click", closeConfig);
    $("etl-config-modal").addEventListener("click", function (event) {
      if (event.target === this) closeConfig();
    });
    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape") closeConfig();
    });

    api("jobs/").then(function (res) {
      if (!res.ok) {
        banner("Could not load the ETL job list: " +
          (res.body.error || "HTTP " + res.status));
        return;
      }
      loadJobs(res.body);
    }).catch(function (err) {
      banner("Could not reach the ETL API: " + err.message);
    });

    refreshHistory();
  }

  function loadJobs(data) {
    state.defaults = data.defaults || {};
    state.configKeys = data.config_keys || state.configKeys;

    var select = $("etl-job");
    select.textContent = "";
    var jobs = data.jobs || [];

    if (!jobs.length) {
      select.appendChild(el("option", null, "No ETL jobs discovered"));
      select.disabled = true;
    } else {
      select.appendChild(el("option", null, "Choose an ETL job…")).value = "";
      jobs.forEach(function (job) {
        state.jobs[job.key] = job;
        var option = el("option", null,
          job.label + "  ·  " + job.key + (job.available ? "" : "  (unavailable)"));
        option.value = job.key;
        option.disabled = !job.available;
        select.appendChild(option);
      });
    }

    var problems = (data.problems || []).slice();
    if (!data.ok) {
      banner("The ETL source could not be read, so no job can be started.",
        null, problems);
    } else if (problems.length) {
      banner("Discovered the ETL jobs with warnings:", "info", problems);
    }

    applyState(data.state);

    var remembered = sessionStorage.getItem("etl.last_job");
    if (remembered && state.jobs[remembered] && state.jobs[remembered].available) {
      select.value = remembered;
      onSelectJob();
    }
  }

  /* ---------------- form rendering ---------------- */

  function onSelectJob() {
    var key = $("etl-job").value;
    state.current = state.jobs[key] || null;
    $("etl-fields").textContent = "";
    formMessage("");

    var keyBadge = $("etl-job-key");
    var route = $("etl-route");
    var notes = $("etl-notes");

    if (!state.current) {
      $("etl-actions").hidden = true;
      route.hidden = true;
      notes.hidden = true;
      keyBadge.hidden = true;
      $("etl-description").textContent =
        "Choose an ETL job above. Its parameters are read from the ETL source, " +
        "so only the ones that job actually accepts are shown.";
      if (!state.run) renderSteps([], 0, "idle");
      return;
    }

    sessionStorage.setItem("etl.last_job", key);
    var job = state.current;

    $("etl-description").textContent = job.description;
    keyBadge.textContent = job.key;
    keyBadge.hidden = false;

    route.hidden = false;
    route.querySelector('[data-role="source"]').textContent = job.source;
    route.querySelector('[data-role="destination"]').textContent = job.destination;

    notes.textContent = "";
    if (job.notes && job.notes.length) {
      job.notes.forEach(function (n) { notes.appendChild(el("li", null, n)); });
      notes.hidden = false;
    } else {
      notes.hidden = true;
    }

    renderFields(job);
    $("etl-actions").hidden = false;
    if (!state.run || state.run.status !== "running") {
      renderSteps(job.steps, 0, "idle");
    }
    updateButtons();
  }

  function renderFields(job) {
    var host = $("etl-fields");
    var groups = {};
    var order = [];

    job.fields.forEach(function (field) {
      if (!groups[field.group]) { groups[field.group] = []; order.push(field.group); }
      groups[field.group].push(field);
    });

    order.forEach(function (name) {
      var section = el("div", "etl-group");
      section.appendChild(el("div", "etl-group-title", name));
      groups[name].forEach(function (field) {
        section.appendChild(renderField(field));
      });
      host.appendChild(section);
    });

    applyDependencies();
  }

  function renderField(field) {
    var wrap = el("div", "etl-field");
    wrap.dataset.field = field.name;
    if (field.depends_on) {
      wrap.dataset.dependsField = field.depends_on.field;
      wrap.dataset.dependsValue = JSON.stringify(field.depends_on.equals);
    }

    var control;
    var inputId = "etl-f-" + field.name;

    if (field.type === "checkbox") {
      control = el("input");
      control.type = "checkbox";
      control.id = inputId;
      control.checked = field.default === true;

      var check = el("label", "etl-check");
      check.htmlFor = inputId;
      check.appendChild(control);
      check.appendChild(el("span", null, field.label));
      wrap.appendChild(check);
    } else {
      var label = el("label");
      label.htmlFor = inputId;
      label.appendChild(el("span", null, field.label));
      if (field.required) {
        label.appendChild(el("span", "etl-required", "*"));
      } else {
        label.appendChild(el("span", "etl-optional", "optional"));
      }
      wrap.appendChild(label);

      if (field.type === "textarea") {
        control = el("textarea", "etl-textarea");
      } else if (field.type === "select") {
        control = el("select", "etl-input");
        control.appendChild(el("option", null, "Choose…")).value = "";
        optionsFor(field).forEach(function (opt) {
          control.appendChild(el("option", null, opt.label)).value = opt.value;
        });
      } else {
        control = el("input", "etl-input");
        control.type = field.type === "password" ? "password"
          : field.type === "number" ? "number" : "text";
      }
      control.id = inputId;
      if (field.placeholder) control.placeholder = field.placeholder;

      var initial = field.default;
      if (field.name === "base_dir" && (initial == null || initial === "")) {
        initial = state.defaults.base_dir || "";
      }
      if (initial != null && initial !== false) control.value = initial;

      wrap.appendChild(control);
    }

    control.name = field.name;
    control.dataset.type = field.type;
    control.addEventListener("input", function () {
      control.classList.remove("is-invalid");
      var err = wrap.querySelector(".etl-field-error");
      if (err) err.remove();
    });
    if (field.type === "checkbox") {
      control.addEventListener("change", applyDependencies);
    }

    if (field.help) wrap.appendChild(el("div", "etl-field-help", field.help));
    return wrap;
  }

  function optionsFor(field) {
    if (field.options_from) {
      var keys = state.configKeys[field.options_from] || [];
      if (!keys.length && state.configKeys.message) {
        return [];
      }
      return keys.map(function (k) { return { value: k, label: k }; });
    }
    return field.options || [];
  }

  function applyDependencies() {
    document.querySelectorAll(".etl-field[data-depends-field]").forEach(function (node) {
      var source = document.querySelector(
        '[name="' + node.dataset.dependsField + '"]');
      if (!source) return;
      var actual = source.type === "checkbox" ? source.checked : source.value;
      var expected = JSON.parse(node.dataset.dependsValue);
      node.hidden = actual !== expected;
    });
  }

  function collectValues() {
    var values = {};
    if (!state.current) return values;

    state.current.fields.forEach(function (field) {
      var wrap = document.querySelector(
        '.etl-field[data-field="' + field.name + '"]');
      if (!wrap || wrap.hidden) return;             // dependency not satisfied
      var control = wrap.querySelector("[name]");
      if (!control) return;
      if (field.type === "checkbox") {
        values[field.name] = control.checked;
      } else {
        var raw = control.value.trim();
        if (raw !== "") values[field.name] = raw;
      }
    });
    return values;
  }

  function showFieldErrors(errors) {
    document.querySelectorAll(".etl-field-error").forEach(function (n) { n.remove(); });
    document.querySelectorAll(".is-invalid").forEach(function (n) {
      n.classList.remove("is-invalid");
    });

    var unattached = [];
    Object.keys(errors || {}).forEach(function (name) {
      var wrap = document.querySelector('.etl-field[data-field="' + name + '"]');
      if (!wrap) { unattached.push(errors[name]); return; }
      var control = wrap.querySelector("[name]");
      if (control) control.classList.add("is-invalid");
      wrap.appendChild(el("div", "etl-field-error", errors[name]));
    });
    return unattached;
  }

  /* ---------------- actions ---------------- */

  function onReset() {
    if (!state.current) return;
    $("etl-fields").textContent = "";
    renderFields(state.current);
    formMessage("Form reset.", "ok");
    banner("");
  }

  function onValidate() {
    if (!state.current) return;
    formMessage("Checking…");
    api("validate/", { method: "POST",
      body: { etl: state.current.key, values: collectValues() } })
      .then(function (res) {
        if (res.status === 400) {
          var extra = showFieldErrors(res.body.errors);
          formMessage("Fix the highlighted fields.", "error");
          if (extra.length) banner(extra.join(" "));
          return;
        }
        if (!res.ok) {
          formMessage(res.body.error || "Validation failed.", "error");
          return;
        }
        showFieldErrors({});
        var findings = res.body.findings || [];
        formMessage(res.body.message, res.body.ok ? "ok" : "error");
        if (findings.length) {
          banner("Environment check:", res.body.ok ? "info" : null,
            findings.map(function (f) {
              return f.level.toUpperCase() + " — " + f.message;
            }));
        } else {
          banner("");
        }
      });
  }

  function onRun() {
    if (!state.current) return;
    banner("");
    formMessage("Starting…");
    $("etl-run-btn").disabled = true;

    api("run/", { method: "POST",
      body: { etl: state.current.key, values: collectValues() } })
      .then(function (res) {
        if (res.status === 400) {
          var extra = showFieldErrors(res.body.errors);
          formMessage("Fix the highlighted fields.", "error");
          if (extra.length) banner(extra.join(" "));
          updateButtons();
          return;
        }
        if (res.status === 409) {
          state.busy = true;
          banner(res.body.error ||
            "Another ETL run is already in progress. Only one runs at a time.");
          formMessage("Blocked by the server: one ETL at a time.", "error");
          watchActive();
          updateButtons();
          return;
        }
        if (!res.ok) {
          formMessage(res.body.error || ("HTTP " + res.status), "error");
          banner(res.body.error || "The ETL could not be started.");
          updateButtons();
          return;
        }
        showFieldErrors({});
        formMessage("Run started.", "ok");
        adoptRun(res.body.run);
        poll();
      })
      .catch(function (err) {
        formMessage("Could not reach the server: " + err.message, "error");
        updateButtons();
      });
  }

  function onStop() {
    var button = $("etl-stop-btn");
    button.disabled = true;
    $("etl-stop-hint").textContent = "Killing the ETL process group…";

    api("stop/", { method: "POST",
      body: { run: state.run ? state.run.id : null } })
      .then(function (res) {
        if (!res.body.ok) {
          banner(res.body.error || "Could not stop the run.");
        }
        $("etl-stop-hint").textContent =
          res.body.message || "Stop kills the ETL process group immediately.";
        tick();
      })
      .catch(function (err) {
        banner("Stop request failed: " + err.message);
        updateButtons();
      });
  }

  /* ---------------- run state ---------------- */

  function adoptRun(run) {
    state.run = run;
    state.cursor = 0;
    $("etl-logs").textContent = "";
    renderRun(run);
    appendLogs(run.logs, run.cursor);
  }

  function watchActive() {
    api("status/").then(function (res) {
      if (res.body && res.body.run) {
        adoptRun(res.body.run);
        poll();
      } else {
        applyState(res.body && res.body.state);
      }
    });
  }

  function poll() {
    clearTimeout(state.timer);
    state.timer = setTimeout(tick, POLL_MS);
  }

  function tick() {
    if (!state.run) return;
    api("status/?run=" + encodeURIComponent(state.run.id) +
        "&since=" + state.cursor)
      .then(function (res) {
        if (!res.ok || !res.body.run) {
          applyState(res.body && res.body.state);
          return;
        }
        var run = res.body.run;
        var lines = run.logs || [];
        state.run = run;
        renderRun(run);
        appendLogs(lines, run.cursor);
        applyState(res.body.state);
        if (run.status === "running") {
          poll();
        } else {
          refreshHistory();
        }
      })
      .catch(function () { poll(); });   // a blip should not stop the watch
  }

  function applyState(runState) {
    state.busy = !!(runState && runState.busy);
    if (runState && runState.busy && !runState.run && runState.lock) {
      banner("An ETL run started by another process is still executing (" +
        (runState.lock.etl || "unknown job") + ", started " +
        fmtTime(runState.lock.started_at) + "). Only one runs at a time.", "info");
    }
    updateButtons();
  }

  function updateButtons() {
    var run = state.run;
    var running = !!(run && run.status === "running");
    $("etl-run-btn").disabled = !state.current || state.busy || running;
    $("etl-stop-btn").disabled = !(running || (state.busy && !run));
    $("etl-validate-btn").disabled = !state.current;
  }

  function renderRun(run) {
    var chip = $("etl-status-chip");
    chip.className = "etl-chip etl-chip-" + run.status;
    chip.textContent = run.status.charAt(0).toUpperCase() + run.status.slice(1);

    $("etl-run-empty").hidden = true;
    $("etl-run-meta").hidden = false;
    $("etl-log-wrap").hidden = false;

    $("etl-meta-job").textContent = run.label + " (" + run.etl + ")";
    $("etl-meta-started").textContent = fmtTime(run.started_at);
    $("etl-meta-duration").textContent = fmtDuration(run.duration_seconds);
    $("etl-meta-trigger").textContent = run.triggered_by || "—";
    $("etl-meta-step").textContent = run.step_name ||
      (run.status === "running" ? "starting…" : "—");
    var pid = $("etl-meta-pid");
    pid.className = "etl-mono";
    pid.textContent = run.pid ? "pid " + run.pid : "—";

    var link = $("etl-log-link");
    link.href = API + "log/?run=" + encodeURIComponent(run.id);
    link.hidden = false;

    renderSteps(run.steps, run.step_index, run.status);
    renderResult(run);
  }

  /* The ETL increments its step bar when it ENTERS a stage, so a count of N
     means stage N (1-based) is the one being worked on — not that N stages are
     finished. Steps before it are done; the rest are pending. */
  function renderSteps(steps, index, status) {
    var host = $("etl-steps");
    host.textContent = "";
    if (!steps || !steps.length) { host.hidden = true; return; }
    host.hidden = false;

    var completed = status === "completed";
    var current = index - 1;                 // 0-based index of the live stage

    steps.forEach(function (name, i) {
      var done = completed || i < current;
      var active = !completed && i === current;
      var cls = "etl-step";
      if (done) cls += " etl-step-done";
      else if (active) cls += status === "running" ? " etl-step-active"
                                                   : " etl-step-halted";
      var item = el("li", cls);
      item.appendChild(el("span", "etl-step-mark", done ? "✓" : ""));
      item.appendChild(el("span", null, name));
      item.appendChild(el("span", "etl-step-count", (i + 1) + "/" + steps.length));
      host.appendChild(item);
    });
  }

  function renderResult(run) {
    var box = $("etl-result");
    box.textContent = "";

    if (run.status === "running") { box.hidden = true; return; }

    if (run.error_detail && run.error_detail.message) {
      box.className = "etl-result etl-result-error";
      box.appendChild(el("div", "etl-result-title",
        run.status === "stopped" ? "Stopped" : run.error_detail.type || "Error"));
      box.appendChild(el("div", null, run.error_detail.message));
      box.hidden = false;
      return;
    }
    if (run.status === "stopped") {
      box.className = "etl-result";
      box.appendChild(el("div", "etl-result-title", "Stopped"));
      box.appendChild(el("div", null,
        "The ETL process group was terminated on request."));
      box.hidden = false;
      return;
    }
    if (run.result) {
      box.className = "etl-result";
      box.appendChild(el("div", "etl-result-title", "Result"));
      box.appendChild(el("div", null, JSON.stringify(run.result, null, 2)));
      box.hidden = false;
      return;
    }
    box.hidden = true;
  }

  function appendLogs(lines, cursor) {
    if (!lines || !lines.length) {
      if (cursor != null) state.cursor = Math.max(state.cursor, cursor);
      return;
    }
    var pane = $("etl-logs");
    var atBottom = $("etl-follow-log").checked;

    lines.forEach(function (line) {
      var row = el("div", line.stream === "err" ? "etl-log-line-err" : null);
      row.appendChild(el("span", "etl-log-ts", line.ts));
      row.appendChild(document.createTextNode(line.text));
      pane.appendChild(row);
      if (line.seq > state.cursor) state.cursor = line.seq;
    });
    if (cursor != null) state.cursor = Math.max(state.cursor, cursor);
    if (atBottom) pane.scrollTop = pane.scrollHeight;
  }

  /* ---------------- history ---------------- */

  function refreshHistory() {
    api("history/").then(function (res) {
      if (!res.ok) return;
      $("etl-log-dir").textContent = res.body.log_dir || "—";
      $("etl-log-dir").title = "Run logs: " + (res.body.log_dir || "");
      renderHistory(res.body.runs || []);
      applyState(res.body.state);
    });
  }

  function renderHistory(runs) {
    var body = $("etl-history-rows");
    body.textContent = "";

    if (!runs.length) {
      var empty = el("tr");
      var cell = el("td", "etl-table-empty",
        "No runs recorded in this application session yet.");
      cell.colSpan = 8;
      empty.appendChild(cell);
      body.appendChild(empty);
      return;
    }

    runs.forEach(function (run) {
      var row = el("tr");
      row.appendChild(el("td", null, run.label));
      row.appendChild(el("td", "etl-cell-time", fmtTime(run.started_at)));
      row.appendChild(el("td", "etl-cell-time", fmtTime(run.finished_at)));
      row.appendChild(el("td", "etl-col-num", fmtDuration(run.duration_seconds)));

      var statusCell = el("td");
      var chip = el("span", "etl-chip etl-chip-" + run.status,
        run.status.charAt(0).toUpperCase() + run.status.slice(1));
      statusCell.appendChild(chip);
      row.appendChild(statusCell);

      row.appendChild(el("td", null, run.triggered_by || "—"));

      var message = el("td", "etl-cell-msg", run.error || "—");
      if (run.error) message.title = run.error;
      row.appendChild(message);

      var logCell = el("td");
      var link = el("a", null, "View");
      link.href = API + "log/?run=" + encodeURIComponent(run.id);
      link.target = "_blank";
      link.rel = "noopener";
      link.title = run.log_path || "";
      logCell.appendChild(link);
      row.appendChild(logCell);

      body.appendChild(row);
    });
  }

  /* ---------------- configuration (read-only) ---------------- */

  function openConfig() {
    $("etl-config-modal").hidden = false;
    var body = $("etl-config-body");
    body.textContent = "";
    body.appendChild(el("p", "etl-empty", "Loading…"));

    api("config/").then(function (res) {
      $("etl-config-source").textContent = res.body.source || "";
      renderConfig(res.body);
    });
  }

  function closeConfig() { $("etl-config-modal").hidden = true; }

  function renderConfig(data) {
    var body = $("etl-config-body");
    body.textContent = "";

    if (data.message) {
      body.appendChild(el("p", "etl-empty", data.message));
    }

    section(body, "Runtime", (data.runtime || []).map(function (row) {
      return { label: row.label, value: row.value, level: row.level };
    }));

    section(body, "Spark cluster", (data.spark || []).map(function (row) {
      return { label: row.label, value: row.value, level: "ok" };
    }));

    if ((data.profiles || []).length) {
      var wrap = el("div", "etl-cfg-section");
      wrap.appendChild(el("div", "etl-cfg-title", "Connection profiles"));
      var rows = el("div", "etl-cfg-rows");
      data.profiles.forEach(function (profile) {
        var item = el("dl", "etl-cfg-row");
        item.appendChild(el("dt", null, profile.name + " · " + profile.kind));
        var value = el("dd", "etl-level-ok");
        value.appendChild(document.createTextNode(
          profile.user + " @ " + profile.endpoint));
        value.appendChild(el("br"));
        value.appendChild(el("span", "etl-level-" + profile.credential_level,
          profile.credential));
        item.appendChild(value);
        rows.appendChild(item);
      });
      wrap.appendChild(rows);
      body.appendChild(wrap);
    }

    if ((data.problems || []).length) {
      var list = el("ul", "etl-cfg-problems");
      data.problems.forEach(function (p) { list.appendChild(el("li", null, p)); });
      body.appendChild(list);
    }
  }

  function section(host, title, rows) {
    if (!rows.length) return;
    var wrap = el("div", "etl-cfg-section");
    wrap.appendChild(el("div", "etl-cfg-title", title));
    var grid = el("div", "etl-cfg-rows");
    rows.forEach(function (row) {
      var item = el("dl", "etl-cfg-row");
      item.appendChild(el("dt", null, row.label));
      item.appendChild(el("dd", "etl-level-" + (row.level || "ok"), row.value));
      grid.appendChild(item);
    });
    wrap.appendChild(grid);
    host.appendChild(wrap);
  }

  /* ---------------- go ---------------- */

  boot();
  // Pick up a run that was already going when this page loaded.
  watchActive();
})();
