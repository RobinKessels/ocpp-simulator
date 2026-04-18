const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

async function api(path, options) {
  const res = await fetch(`${API_BASE}${path}`, options || {});
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || "Request failed");
  return data;
}

async function remoteStart(cpId, evseId) {
  try {
    await api(`/remote-start/${cpId}/${evseId}`, { method: "POST" });
    await refreshStations();
  } catch (err) {
    alert(`Start failed: ${err.message}`);
  }
}

async function remoteStop(cpId, evseId) {
  try {
    await api(`/remote-stop/${cpId}/${evseId}`, { method: "POST" });
    await refreshStations();
  } catch (err) {
    alert(`Stop failed: ${err.message}`);
  }
}

function evseCard(cpId, evse) {
  const soc = evse.soc_percent == null ? "-" : `${evse.soc_percent}%`;
  const tx = evse.transaction_id || "-";
  const amps = evse.assigned_amps || 0;
  const isCharging = evse.status === "Charging";
  const actionButton = isCharging
    ? `<button class="stop" onclick="remoteStop('${cpId}', ${evse.evse_id})">Stop</button>`
    : `<button onclick="remoteStart('${cpId}', ${evse.evse_id})">Start Charging</button>`;
  return `
    <div class="evse">
      <div class="evse-title">
        <strong>EVSE ${evse.evse_id}</strong>
        <span class="amps-badge">${amps} A</span>
      </div>
      <div>Status: <span class="${evse.status === "Charging" ? "ok" : ""}">${evse.status}</span></div>
      <div>SoC: ${soc}</div>
      <div>Transaction: ${tx}</div>
      <div class="muted">Last event: ${evse.last_event_type || "-"}</div>
      <div class="row" style="margin-top: 10px; margin-bottom: 0;">
        ${actionButton}
      </div>
    </div>
  `;
}

function stationCard(station) {
  const totalAssigned = station.evses.reduce((sum, evse) => sum + (evse.assigned_amps || 0), 0);
  const evseHtml = station.evses.map((evse) => evseCard(station.cp_id, evse)).join("");
  return `
    <div class="card">
      <div class="station-header">
        <strong>${station.cp_id}</strong>
        <span class="station-amps">Total Assigned ${totalAssigned} A</span>
      </div>
      <div class="evse-grid">${evseHtml}</div>
    </div>
  `;
}

async function refreshStations() {
  try {
    const [data, site] = await Promise.all([api("/stations"), api("/site-state")]);
    const html = data.stations.map((station) => stationCard(station)).join("");
    document.getElementById("stations").innerHTML = html || '<div class="muted">No connected chargers</div>';
    const utilization =
      site.site_max_amps > 0
        ? Math.round((site.total_allocated_amps / site.site_max_amps) * 100)
        : 0;
    document.getElementById("siteState").innerHTML =
      `<strong>Site Load</strong> · Max ${site.site_max_amps} A · Active Sessions ${site.active_sessions} · ` +
      `Per Active EVSE ${site.assigned_amps_per_active_evse} A · Allocated ${site.total_allocated_amps} A (${utilization}%)`;
    document.getElementById("updated").textContent = `Updated: ${new Date().toLocaleTimeString()}`;
  } catch (err) {
    document.getElementById("stations").innerHTML = `<div class="muted">Failed to load stations: ${err.message}</div>`;
  }
}

document.getElementById("refreshBtn").addEventListener("click", refreshStations);
window.remoteStart = remoteStart;
window.remoteStop = remoteStop;
refreshStations();
setInterval(refreshStations, 3000);
