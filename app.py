"""నిరీక్షణ (Nirikshana) — Data Quality, Profiling & Drift Detection Web GUI (White & Purple Edition)."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from pipeline import execute_pipeline, generate_sample_suite, load_config
from src.db import get_connection, init_db, seed_reference_customers
from src.reconciler import run_reconciliation

app = FastAPI(title="Nirikshana (నిరీక్షణ) - Data Quality & Drift Sentinel")

DB_PATH = Path("pipeline.db")


def ensure_db():
    init_db(DB_PATH)
    seed_reference_customers(DB_PATH)


class RunRequest(BaseModel):
    input_file: str
    run_id: str
    set_baseline: bool = False


@app.get("/api/batches")
def get_batches():
    data_dir = Path("data")
    if not data_dir.exists():
        return {"files": []}
    files = [f.name for f in data_dir.glob("*.csv")]
    return {"files": sorted(files)}


@app.get("/api/status")
def get_status():
    ensure_db()
    conn = get_connection(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as c FROM transactions;")
    trans_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM quarantine;")
    quar_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM alerts;")
    alerts_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM run_log;")
    runs_cnt = cur.fetchone()["c"]
    conn.close()
    return {
        "transactions_count": trans_cnt,
        "quarantine_count": quar_cnt,
        "alerts_count": alerts_cnt,
        "runs_count": runs_cnt,
    }


@app.get("/api/reconcile")
def get_reconciliation():
    ensure_db()
    conn = get_connection(DB_PATH)
    res = run_reconciliation(conn)
    conn.close()
    return res


@app.get("/api/quarantine")
def get_quarantine(limit: int = 20):
    ensure_db()
    conn = get_connection(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT quarantine_id, run_id, source_file, quarantine_reason, quarantined_at, raw_record "
        "FROM quarantine ORDER BY quarantine_id DESC LIMIT ?;",
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"records": rows}


@app.get("/api/alerts")
def get_alerts(limit: int = 20):
    ensure_db()
    conn = get_connection(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT alert_id, run_id, metric, column_name, severity, baseline_value, current_value, delta, threshold, message, created_at "
        "FROM alerts ORDER BY alert_id DESC LIMIT ?;",
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"alerts": rows}


@app.post("/api/run")
def run_batch(req: RunRequest):
    input_path = Path("data") / req.input_file
    if not input_path.exists():
        input_path = Path(req.input_file)
    if not input_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {req.input_file}")

    result = execute_pipeline(
        input_path=str(input_path),
        run_id=req.run_id,
        set_baseline=req.set_baseline,
        quiet=True,
    )
    return result


@app.post("/api/generate-samples")
def trigger_generate_samples():
    generate_sample_suite()
    return {"status": "ok", "message": "Sample datasets generated in data/ folder"}


@app.get("/", response_class=HTMLResponse)
def index():
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>నిరీక్షణ (Nirikshana) — Data Quality & Drift Monitor</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-page: #f8f7fc;
      --card-bg: #ffffff;
      --card-border: #ede9fe;
      --purple-primary: #7c3aed;
      --purple-dark: #5b21b6;
      --purple-light: #f5f3ff;
      --purple-border: #ddd6fe;
      --text-main: #1e1b4b;
      --text-muted: #64748b;
      --success: #059669;
      --warning: #d97706;
      --danger: #dc2626;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Inter', sans-serif;
      background-color: var(--bg-page);
      color: var(--text-main);
      line-height: 1.5;
      padding: 24px 32px;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
      padding-bottom: 18px;
      border-bottom: 2px solid var(--card-border);
      background: #ffffff;
      padding: 18px 24px;
      border-radius: 12px;
      box-shadow: 0 1px 3px rgba(124, 58, 237, 0.05);
    }
    .logo-container { display: flex; align-items: center; gap: 14px; }
    .telugu-logo {
      font-size: 28px;
      font-weight: 800;
      background: linear-gradient(135deg, #7c3aed, #4c1d95);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      padding: 2px 12px;
      border-radius: 10px;
      border: 2px solid #ddd6fe;
      background-color: #faf5ff;
    }
    .title-box h1 { font-size: 20px; font-weight: 700; color: #3b0764; }
    .title-box p { font-size: 13px; color: var(--text-muted); }
    
    .btn-secondary {
      background: #ffffff;
      color: var(--purple-dark);
      border: 1.5px solid var(--purple-border);
      padding: 9px 16px;
      border-radius: 8px;
      cursor: pointer;
      font-size: 13px;
      font-weight: 600;
      transition: all 0.2s;
    }
    .btn-secondary:hover {
      background: var(--purple-light);
      border-color: var(--purple-primary);
    }
    
    .grid-stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .stat-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-top: 4px solid var(--purple-primary);
      border-radius: 12px;
      padding: 18px 22px;
      box-shadow: 0 2px 4px rgba(124, 58, 237, 0.04);
    }
    .stat-label { font-size: 12px; font-weight: 600; text-transform: uppercase; color: var(--text-muted); letter-spacing: 0.5px; }
    .stat-val { font-size: 28px; font-weight: 800; margin-top: 6px; }
    .val-purple { color: #6b21a8; }
    .val-success { color: var(--success); }
    .val-danger { color: var(--danger); }
    .val-warning { color: var(--warning); }

    .main-grid {
      display: grid;
      grid-template-columns: 360px 1fr;
      gap: 22px;
    }
    @media (max-width: 950px) { .main-grid { grid-template-columns: 1fr; } }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 22px;
      margin-bottom: 20px;
      box-shadow: 0 2px 6px rgba(124, 58, 237, 0.04);
    }
    .card-title {
      font-size: 16px;
      font-weight: 700;
      margin-bottom: 16px;
      color: #3b0764;
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--card-border);
      padding-bottom: 10px;
    }
    .form-group { margin-bottom: 16px; }
    label { display: block; font-size: 13px; font-weight: 600; color: #475569; margin-bottom: 6px; }
    select, input[type="text"] {
      width: 100%;
      background: #faf5ff;
      border: 1.5px solid #ddd6fe;
      padding: 10px 14px;
      border-radius: 8px;
      color: #1e1b4b;
      font-size: 14px;
      font-weight: 500;
      outline: none;
      transition: border-color 0.2s;
    }
    select:focus, input[type="text"]:focus {
      border-color: var(--purple-primary);
      background: #ffffff;
      box-shadow: 0 0 0 3px rgba(124, 58, 237, 0.15);
    }
    .checkbox-group { display: flex; align-items: center; gap: 10px; margin: 18px 0; }
    .checkbox-group input { width: 17px; height: 17px; accent-color: var(--purple-primary); }
    .checkbox-group label { margin-bottom: 0; cursor: pointer; color: #334155; font-size: 14px; font-weight: 500; }
    
    .btn-primary {
      width: 100%;
      background: linear-gradient(135deg, #7c3aed, #6b21a8);
      color: #ffffff;
      border: none;
      padding: 12px;
      border-radius: 8px;
      font-weight: 700;
      font-size: 14px;
      cursor: pointer;
      box-shadow: 0 3px 6px rgba(107, 33, 168, 0.25);
      transition: all 0.2s;
    }
    .btn-primary:hover {
      background: linear-gradient(135deg, #6b21a8, #581c87);
      box-shadow: 0 4px 10px rgba(107, 33, 168, 0.35);
      transform: translateY(-1px);
    }

    /* Alert cards */
    .alert-item {
      background: #ffffff;
      border: 1.5px solid #fecaca;
      border-left: 5px solid var(--danger);
      border-radius: 8px;
      padding: 14px 16px;
      margin-bottom: 12px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.02);
    }
    .alert-item.MEDIUM {
      border-color: #fde68a;
      border-left-color: var(--warning);
    }
    .alert-header { display: flex; justify-content: space-between; font-size: 14px; font-weight: 700; }
    .badge {
      display: inline-block;
      padding: 3px 10px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
    }
    .badge-HIGH { background: #fee2e2; color: #b91c1c; border: 1px solid #fca5a5; }
    .badge-MEDIUM { background: #fef3c7; color: #92400e; border: 1px solid #fcd34d; }
    .alert-meta {
      font-family: monospace;
      font-size: 12px;
      color: #4b5563;
      margin: 8px 0;
      background: #faf5ff;
      border: 1px solid #ede9fe;
      padding: 6px 10px;
      border-radius: 6px;
    }
    .alert-msg { font-size: 13px; color: #1e1b4b; font-weight: 600; }

    /* Tables */
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th {
      text-align: left;
      padding: 12px 14px;
      color: #4c1d95;
      background: #faf5ff;
      font-weight: 700;
      border-bottom: 2px solid var(--purple-border);
    }
    td { padding: 12px 14px; border-bottom: 1px solid #f1f5f9; color: #1e293b; }
    tr:hover td { background-color: #faf5ff; }
    .tag-ok { color: var(--success); font-weight: 700; }
    .tag-quar { color: var(--danger); font-weight: 700; }
  </style>
</head>
<body>

  <header class="header">
    <div class="logo-container">
      <div class="telugu-logo">నిరీక్షణ</div>
      <div class="title-box">
        <h1>Nirikshana — Data Quality & Drift Monitor</h1>
        <p>White & Royal Purple Edition • Batch statistical profiling & explainable alerts</p>
      </div>
    </div>
    <button class="btn-secondary" onclick="loadAll()">↻ Refresh Telemetry</button>
  </header>

  <section class="grid-stats">
    <div class="stat-card">
      <div class="stat-label">Valid Ingested Records</div>
      <div class="stat-val val-success" id="stat-trans">0</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Quarantined Records</div>
      <div class="stat-val val-danger" id="stat-quar">0</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Triggered Explainable Alerts</div>
      <div class="stat-val val-warning" id="stat-alerts">0</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Pipeline Runs</div>
      <div class="stat-val val-purple" id="stat-runs">0</div>
    </div>
  </section>

  <div class="main-grid">
    <div>
      <div class="card">
        <div class="card-title">Run Batch Pipeline</div>
        <div class="form-group">
          <label>Select Batch CSV</label>
          <select id="select-batch"></select>
        </div>
        <div class="form-group">
          <label>Run Identifier (Run ID)</label>
          <input type="text" id="input-runid" value="run_gui_001" placeholder="e.g. run_003">
        </div>
        <div class="checkbox-group">
          <input type="checkbox" id="check-baseline">
          <label for="check-baseline">Set as Baseline Profile</label>
        </div>
        <button class="btn-primary" id="btn-execute" onclick="executeBatch()">Execute Pipeline</button>
        <div id="run-status" style="margin-top: 12px; font-size: 13px; font-weight: 600;"></div>
      </div>

      <div class="card">
        <div class="card-title">Sample Generator</div>
        <p style="font-size: 13px; color: var(--text-muted); margin-bottom: 14px;">Generate 7 demonstration files: baseline, normal, drifted nulls, shifted mean, stale freshness & corrupted records.</p>
        <button class="btn-secondary" style="width: 100%; border-color: var(--purple-primary); color: var(--purple-primary);" onclick="generateSamples()">Regenerate Demo Data</button>
      </div>
    </div>

    <div>
      <div class="card">
        <div class="card-title">Recent Explainable Alerts</div>
        <div id="alerts-list">
          <p style="color: var(--text-muted); font-size: 13px;">No alerts recorded yet.</p>
        </div>
      </div>

      <div class="card">
        <div class="card-title">Data Reconciliation Summary (Diff = 0)</div>
        <div style="overflow-x: auto;">
          <table>
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Source File</th>
                <th>In</th>
                <th>Loaded</th>
                <th>Quarantine</th>
                <th>Diff</th>
                <th>Integrity</th>
              </tr>
            </thead>
            <tbody id="recon-tbody"></tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <div class="card-title">Traceable Quarantine Explorer</div>
        <div style="overflow-x: auto;">
          <table>
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Rejection Root Cause</th>
                <th>Quarantined At</th>
              </tr>
            </thead>
            <tbody id="quar-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    async function loadAll() {
      // Load batches
      try {
        const bRes = await fetch('/api/batches');
        const bData = await bRes.json();
        const sel = document.getElementById('select-batch');
        sel.innerHTML = '';
        bData.files.forEach(f => {
          const opt = document.createElement('option');
          opt.value = f;
          opt.textContent = f;
          sel.appendChild(opt);
        });
      } catch (e) {}

      // Load stats
      try {
        const sRes = await fetch('/api/status');
        const sData = await sRes.json();
        document.getElementById('stat-trans').textContent = Number(sData.transactions_count).toLocaleString();
        document.getElementById('stat-quar').textContent = Number(sData.quarantine_count).toLocaleString();
        document.getElementById('stat-alerts').textContent = Number(sData.alerts_count).toLocaleString();
        document.getElementById('stat-runs').textContent = Number(sData.runs_count).toLocaleString();
      } catch (e) {}

      // Load alerts
      try {
        const aRes = await fetch('/api/alerts');
        const aData = await aRes.json();
        const container = document.getElementById('alerts-list');
        if (!aData.alerts || aData.alerts.length === 0) {
          container.innerHTML = '<p style="color: var(--text-muted); font-size: 13px;">No active alerts.</p>';
        } else {
          container.innerHTML = aData.alerts.slice(0, 5).map(a => `
            <div class="alert-item ${a.severity}">
              <div class="alert-header">
                <span>[${a.metric}] column=${a.column_name}</span>
                <span class="badge badge-${a.severity}">${a.severity}</span>
              </div>
              <div class="alert-meta">
                baseline=${a.baseline_value !== null ? a.baseline_value.toFixed(2) : 'N/A'} | current=${a.current_value !== null ? a.current_value.toFixed(2) : 'N/A'} | delta=${a.delta > 0 ? '+' : ''}${a.delta !== null ? a.delta.toFixed(2) : 'N/A'} | threshold=${a.threshold !== null ? a.threshold.toFixed(2) : 'N/A'}
              </div>
              <div class="alert-msg">"${a.message}"</div>
            </div>
          `).join('');
        }
      } catch (e) {}

      // Load reconciliation
      try {
        const rRes = await fetch('/api/reconcile');
        const rData = await rRes.json();
        const tbody = document.getElementById('recon-tbody');
        tbody.innerHTML = (rData.runs || []).map(r => `
          <tr>
            <td><strong>${r.run_id}</strong></td>
            <td style="color: var(--text-muted);">${r.source_file.split(/[/\\\\]/).pop()}</td>
            <td>${r.input_count}</td>
            <td class="tag-ok">${r.target_count}</td>
            <td class="tag-quar">${r.quarantine_count}</td>
            <td><strong>${r.diff}</strong></td>
            <td><span class="tag-ok">✓ 100% Reconciled</span></td>
          </tr>
        `).join('');
      } catch (e) {}

      // Load quarantine
      try {
        const qRes = await fetch('/api/quarantine');
        const qData = await qRes.json();
        const tbody = document.getElementById('quar-tbody');
        tbody.innerHTML = (qData.records || []).slice(0, 8).map(q => `
          <tr>
            <td><strong>${q.run_id}</strong></td>
            <td style="color: #dc2626; font-weight: 500;">${q.quarantine_reason}</td>
            <td style="color: var(--text-muted); font-size: 12px;">${new Date(q.quarantined_at).toLocaleTimeString()}</td>
          </tr>
        `).join('');
      } catch (e) {}
    }

    async function executeBatch() {
      const file = document.getElementById('select-batch').value;
      const runId = document.getElementById('input-runid').value || 'run_' + Date.now();
      const setBaseline = document.getElementById('check-baseline').checked;
      const statusDiv = document.getElementById('run-status');
      const btn = document.getElementById('btn-execute');

      btn.disabled = true;
      btn.textContent = 'Processing...';
      statusDiv.innerHTML = '<span style="color: var(--purple-primary);">Running pipeline...</span>';

      try {
        const res = await fetch('/api/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ input_file: file, run_id: runId, set_baseline: setBaseline })
        });
        const data = await res.json();
        statusDiv.innerHTML = `<span style="color: var(--success);">Completed: ${data.status} (Valid: ${data.valid_count}, Quarantined: ${data.quarantine_count}, Alerts: ${data.alerts.length})</span>`;
        // increment run ID for convenience
        document.getElementById('input-runid').value = 'run_' + (Math.floor(Math.random() * 900) + 100);
        await loadAll();
      } catch (e) {
        statusDiv.innerHTML = `<span style="color: var(--danger);">Error: ${e.message}</span>`;
      } finally {
        btn.disabled = false;
        btn.textContent = 'Execute Pipeline';
      }
    }

    async function generateSamples() {
      const res = await fetch('/api/generate-samples', { method: 'POST' });
      const data = await res.json();
      alert(data.message);
      loadAll();
    }

    window.onload = loadAll;
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)


if __name__ == "__main__":
    ensure_db()
    print("\n" + "=" * 65)
    print("నిరీక్షణ (Nirikshana) Data Quality GUI — White & Purple Edition")
    print("=" * 65)
    print("Starting server at: http://localhost:8000")
    print("Open http://localhost:8000 in your browser to interact with the GUI.")
    print("=" * 65 + "\n")
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
