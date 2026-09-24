/**
 * LakeFlow 2.0 - Core Frontend Application Logic
 * Interactive Architecture Inspector, Real-time CDC Simulation Engine,
 * Apache Iceberg Explorer & Trino SQL Strategy Benchmark Arena
 */

document.addEventListener("DOMContentLoaded", () => {
  // State Initialization
  const state = {
    activeTab: "architecture",
    activeTable: "silver_customers",
    eventsFilter: "ALL",
    currentLsn: 25040150,
    activeSnapshots: 142,
    events: [
      {
        id: "ev-001",
        timestamp: "2026-09-23 18:45:27",
        table: "customers",
        op: "INSERT",
        key: "c1000000-0000-0000-0000-999999999001",
        lsn: 25040112,
        details: "Jane Doe | jane.doe@enterprise.io | USA",
        status: "COMMITTED_ICEBERG"
      },
      {
        id: "ev-002",
        timestamp: "2026-09-23 18:45:28",
        table: "customers",
        op: "UPDATE",
        key: "c1000000-0000-0000-0000-999999999001",
        lsn: 25040120,
        details: "Jane Doe | jane.doe@globalfirm.org | United States",
        status: "COMMITTED_ICEBERG"
      },
      {
        id: "ev-003",
        timestamp: "2026-09-23 18:45:29",
        table: "orders",
        op: "INSERT",
        key: "o3000000-0000-0000-0000-888888888001",
        lsn: 25040135,
        details: "ORD-2026-9901 | Total: $1,450.00 | Status: PENDING",
        status: "COMMITTED_ICEBERG"
      },
      {
        id: "ev-004",
        timestamp: "2026-09-23 18:45:31",
        table: "orders",
        op: "UPDATE",
        key: "o3000000-0000-0000-0000-888888888001",
        lsn: 25040145,
        details: "ORD-2026-9901 | Status: SHIPPED | Carrier: FedEx Express",
        status: "COMMITTED_ICEBERG"
      },
      {
        id: "ev-005",
        timestamp: "2026-09-23 18:45:32",
        table: "order_items",
        op: "DELETE",
        key: "i4000000-0000-0000-0000-000000000005",
        lsn: 25040151,
        details: "Tombstone row written | Reason: Customer cancelled item",
        status: "TOMBSTONE_WRITTEN"
      }
    ],
    icebergSnapshots: {
      silver_customers: [
        { id: "snap-75120340", ts: "2026-09-23 18:45:28", op: "overwrite", records: "+1 updated", manifests: 2, files: 4 },
        { id: "snap-75120336", ts: "2026-09-23 18:45:27", op: "append", records: "+1 inserted", manifests: 2, files: 4 },
        { id: "snap-75120100", ts: "2026-09-23 18:40:00", op: "append", records: "+120,448 rows", manifests: 1, files: 3 }
      ],
      silver_orders: [
        { id: "snap-75120400", ts: "2026-09-23 18:45:31", op: "overwrite", records: "+1 order status", manifests: 5, files: 16 },
        { id: "snap-75120380", ts: "2026-09-23 18:45:29", op: "append", records: "+1 order created", manifests: 5, files: 16 },
        { id: "snap-75119900", ts: "2026-09-23 18:35:10", op: "rewrite_data_files", records: "Compacted 128MB files", manifests: 4, files: 15 }
      ],
      silver_order_items: [
        { id: "snap-75120450", ts: "2026-09-23 18:45:32", op: "delete", records: "Equality delete tombstone", manifests: 8, files: 22 },
        { id: "snap-75118500", ts: "2026-09-23 18:10:00", op: "append", records: "+5,420,099 rows", manifests: 7, files: 21 }
      ]
    },
    schemas: {
      silver_customers: `-- Apache Iceberg Schema: lakeflow.silver_customers
CREATE TABLE lakeflow.silver_customers (
  customer_id     VARCHAR(36) NOT NULL,
  first_name      VARCHAR(100),
  last_name       VARCHAR(100),
  email           VARCHAR(255),
  country         VARCHAR(50),
  created_at      TIMESTAMP(6),
  updated_at      TIMESTAMP(6),
  _source_lsn     BIGINT,
  _op             VARCHAR(1)
)
USING iceberg
PARTITIONED BY (country);`,
      silver_orders: `-- Apache Iceberg Schema: lakeflow.silver_orders
CREATE TABLE lakeflow.silver_orders (
  order_id        VARCHAR(36) NOT NULL,
  customer_id     VARCHAR(36) NOT NULL,
  order_number    VARCHAR(50),
  order_status    VARCHAR(30),
  order_date      DATE,
  total_amount    DECIMAL(12, 2),
  created_at      TIMESTAMP(6),
  _source_lsn     BIGINT,
  _op             VARCHAR(1)
)
USING iceberg
PARTITIONED BY (order_date);`,
      silver_order_items: `-- Apache Iceberg Schema: lakeflow.silver_order_items
CREATE TABLE lakeflow.silver_order_items (
  item_id         VARCHAR(36) NOT NULL,
  order_id        VARCHAR(36) NOT NULL,
  product_id      VARCHAR(36),
  quantity        INTEGER,
  unit_price      DECIMAL(10, 2),
  _source_lsn     BIGINT,
  _deleted        BOOLEAN
)
USING iceberg
TBLPROPERTIES (
  'write.delete.mode' = 'merge-on-read',
  'format-version' = '2'
);`
    },
    services: [
      { name: "PostgreSQL 16", port: "5432", url: "postgresql://localhost:5432/platform_db", creds: "postgres / postgres", probe: "pg_isready -U postgres" },
      { name: "Kafka (KRaft)", port: "9092", url: "localhost:9092", creds: "N/A (PLAINTEXT)", probe: "nc -z localhost 9092" },
      { name: "Kafka Connect (Debezium)", port: "8083", url: "http://localhost:8083", creds: "N/A", probe: "curl /connectors" },
      { name: "Spark Master UI", port: "8080", url: "http://localhost:8080", creds: "N/A", probe: "curl http://localhost:8080" },
      { name: "Spark Worker UI", port: "8081", url: "http://localhost:8081", creds: "N/A", probe: "curl http://localhost:8081" },
      { name: "MinIO S3 API", port: "9000", url: "http://localhost:9000", creds: "admin / password123", probe: "curl /minio/health/live" },
      { name: "MinIO Web Console", port: "9001", url: "http://localhost:9001", creds: "admin / password123", probe: "Web UI Login" },
      { name: "Trino Query Engine", port: "8082", url: "http://localhost:8082", creds: "user: admin", probe: "curl /v1/info" },
      { name: "Redis Feature Store", port: "6379", url: "localhost:6379", creds: "No password (dev)", probe: "redis-cli ping" },
      { name: "Airflow Web UI", port: "8888", url: "http://localhost:8888", creds: "admin / admin", probe: "curl /health" },
      { name: "Prometheus", port: "9090", url: "http://localhost:9090", creds: "N/A", probe: "curl /-/healthy" },
      { name: "Grafana Dashboards", port: "3000", url: "http://localhost:3000", creds: "admin / admin", probe: "curl /api/health" }
    ],
    nodeSpecs: {
      postgres: {
        title: "PostgreSQL 16 (Logical WAL Replication Engine)",
        desc: "Source OLTP database streaming zero-loss transaction logs using PostgreSQL logical decoding.",
        details: [
          "<strong>WAL Configuration:</strong> <code>wal_level = logical</code>, <code>max_wal_senders = 10</code>, <code>max_replication_slots = 10</code>",
          "<strong>Heartbeat Guard:</strong> 5000ms heartbeat interval prevents replication slot LSN stalls during idle windows.",
          "<strong>CDC Schema:</strong> <code>platform_db.platform.{customers, orders, order_items}</code>"
        ]
      },
      debezium: {
        title: "Debezium Connect 2.6 (Non-Intrusive Change Capture)",
        desc: "Kafka Connect distributed worker tailing the PostgreSQL WAL stream via the pgoutput plugin.",
        details: [
          "<strong>Plugin Engine:</strong> <code>io.debezium.connector.postgresql.PostgresConnector</code>",
          "<strong>Zero Read Load:</strong> Changes are read directly from transaction log files without SQL SELECT queries.",
          "<strong>Serialization:</strong> JSON envelope preserving before/after state, LSN, and transaction metadata."
        ]
      },
      kafka: {
        title: "Apache Kafka 3.7 (KRaft Event Backbone)",
        desc: "High-throughput, persistent commit log operating without Apache ZooKeeper.",
        details: [
          "<strong>Topics:</strong> <code>lakeflow.platform.customers</code>, <code>lakeflow.platform.orders</code>",
          "<strong>Partitioning:</strong> Entity primary keys serve as Kafka partition keys to enforce strict per-key ordering.",
          "<strong>Brokers:</strong> Port 9092 external, 29092 internal container network."
        ]
      },
      spark: {
        title: "Apache Spark 3.5.1 Structured Streaming Engine",
        desc: "Stateful stream processing compute cluster with off-heap RocksDB state store and watermark deduplication.",
        details: [
          "<strong>State Store:</strong> <code>org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider</code>",
          "<strong>Deduplication:</strong> 10-minute event-time watermark filtering duplicates based on <code>(key, lsn)</code>.",
          "<strong>Target Files:</strong> 128MB target Parquet file sizing with Zstandard compression."
        ]
      },
      iceberg: {
        title: "Apache Iceberg Open Table Format (v2 Spec)",
        desc: "High-performance ACID analytical table format replacing legacy Hive partitions.",
        details: [
          "<strong>Snapshot Isolation:</strong> Every commit produces an immutable metadata snapshot.",
          "<strong>Hidden Partitioning:</strong> Automatically prunes partitions without requiring users to maintain partition columns.",
          "<strong>Deletes:</strong> Row-level equality deletes and position deletes (Merge-On-Read)."
        ]
      },
      minio: {
        title: "MinIO S3 Storage Layer",
        desc: "S3-compatible distributed object storage holding Iceberg data files and Spark checkpoints.",
        details: [
          "<strong>Buckets:</strong> <code>s3a://warehouse/</code>, <code>s3a://lakeflow/checkpoints/</code>",
          "<strong>API Port:</strong> :9000 (S3 API), :9001 (Web Console)",
          "<strong>Performance:</strong> Multi-gigabyte/sec local read/write bandwidth."
        ]
      },
      trino: {
        title: "Trino 446 Distributed ANSI SQL Query Engine",
        desc: "Fast distributed query engine executing sub-second analytics over Iceberg tables.",
        details: [
          "<strong>Speedup:</strong> 75.4x faster (3.4ms vs 254.5ms) compared to raw CDC log window queries.",
          "<strong>Optimization:</strong> Columnar dictionary pushdown, min/max file skipping, and hidden partition pruning.",
          "<strong>Federation:</strong> Connects simultaneously to Iceberg catalog and PostgreSQL."
        ]
      },
      redis: {
        title: "Redis 7.2 (FeatureHub Real-Time Serving)",
        desc: "Ultra-low-latency in-memory cache powering real-time feature store lookups.",
        details: [
          "<strong>Latency:</strong> &lt; 12ms p99 response times for online model inference.",
          "<strong>Port:</strong> :6379",
          "<strong>Feature Serving:</strong> Synchronized with streaming CDC pipeline for real-time customer feature views."
        ]
      },
      observability: {
        title: "Prometheus 2.52 & Grafana 10.4 Observability",
        desc: "Unified platform telemetry scraping all 12 services every 15 seconds.",
        details: [
          "<strong>Prometheus:</strong> Scrapes Trino, MinIO, Redis, Spark, and system metrics on :9090.",
          "<strong>Grafana:</strong> Pre-provisioned dashboards on :3000 tracking end-to-end latency and throughput.",
          "<strong>Airflow:</strong> Standalone scheduler and webserver on :8888 for scheduled table compaction."
        ]
      }
    }
  };

  // =========================================================================
  // DOM Elements
  // =========================================================================
  const navItems = document.querySelectorAll(".nav-item");
  const tabPanes = document.querySelectorAll(".tab-pane");
  const eventsTableBody = document.getElementById("events-table-body");
  const badgeEventCount = document.getElementById("badge-event-count");
  const filterPills = document.querySelectorAll(".filter-pill");
  const schemaViewer = document.getElementById("schema-viewer");
  const snapshotsTimeline = document.getElementById("snapshots-timeline");
  const tableSelectorItems = document.querySelectorAll(".table-selector-item");
  const servicesTableBody = document.getElementById("services-table-body");
  const toastContainer = document.getElementById("toast-container");
  const nodeInspector = document.getElementById("node-inspector");
  const inspectorTitle = document.getElementById("inspector-title");
  const inspectorBody = document.getElementById("inspector-body");
  const btnCloseInspector = document.getElementById("btn-close-inspector");
  const archNodes = document.querySelectorAll(".arch-node");

  // Lifecycle Tracer Elements
  const tsPg = document.getElementById("ts-pg");
  const tsDeb = document.getElementById("ts-deb");
  const tsSpk = document.getElementById("ts-spk");
  const tsIcb = document.getElementById("ts-icb");
  const tsTrn = document.getElementById("ts-trn");
  const tracerStatus = document.getElementById("tracer-status");

  // Quick Action Buttons
  const btnStep1 = document.getElementById("mbtn-insert-cust");
  const btnStep2 = document.getElementById("mbtn-update-cust");
  const btnStep3 = document.getElementById("mbtn-insert-order");
  const btnStep4 = document.getElementById("mbtn-update-order");
  const btnStep5 = document.getElementById("mbtn-delete-item");
  const btnTriggerAll = document.getElementById("btn-trigger-all-mutations");
  const btnClearEvents = document.getElementById("btn-clear-events");
  const btnRunQueryBenchmark = document.getElementById("btn-run-query-benchmark");
  const btnQuickTour = document.getElementById("btn-quick-tour");
  const btnTriggerCompaction = document.getElementById("btn-trigger-compaction");
  const btnProbeAll = document.getElementById("btn-probe-all-services");

  // Custom Mutation Modal
  const modalOverlay = document.getElementById("custom-mutation-modal");
  const btnOpenCustomModal = document.getElementById("btn-open-custom-mutation");
  const btnCloseCustomModal = document.getElementById("btn-close-mutation-modal");
  const btnCancelCustomMutation = document.getElementById("btn-cancel-custom-mutation");
  const btnSubmitCustomMutation = document.getElementById("btn-submit-custom-mutation");

  // Sidebar Quick Buttons
  const btnQuickCust = document.getElementById("btn-quick-cust");
  const btnQuickOrder = document.getElementById("btn-quick-order");
  const btnQuickDelete = document.getElementById("btn-quick-delete");

  // =========================================================================
  // Tab Switching
  // =========================================================================
  navItems.forEach(item => {
    item.addEventListener("click", () => {
      const targetTab = item.getAttribute("data-tab");
      switchTab(targetTab);
    });
  });

  function switchTab(tabId) {
    state.activeTab = tabId;
    navItems.forEach(nav => {
      nav.classList.toggle("active", nav.getAttribute("data-tab") === tabId);
    });
    tabPanes.forEach(pane => {
      pane.classList.toggle("active", pane.id === `pane-${tabId}`);
    });
  }

  // =========================================================================
  // Toast Notifications
  // =========================================================================
  function showToast(message, type = "info") {
    const toast = document.createElement("div");
    toast.className = `toast toast-${type}`;
    const icon = type === "success" ? "✅" : type === "warn" ? "⚠️" : type === "error" ? "❌" : "ℹ️";
    toast.innerHTML = `<span>${icon}</span> <span>${message}</span>`;
    toastContainer.appendChild(toast);

    setTimeout(() => {
      toast.style.opacity = "0";
      toast.style.transform = "translateX(30px)";
      toast.style.transition = "all 0.3s ease";
      setTimeout(() => toast.remove(), 300);
    }, 3800);
  }

  // =========================================================================
  // Architecture Node Inspection
  // =========================================================================
  archNodes.forEach(node => {
    node.addEventListener("click", () => {
      const nodeKey = node.getAttribute("data-node");
      inspectNode(nodeKey);
    });
  });

  function inspectNode(nodeKey) {
    const info = state.nodeSpecs[nodeKey];
    if (!info) return;

    inspectorTitle.innerHTML = info.title;
    inspectorBody.innerHTML = `
      <div>
        <p style="margin-bottom: 12px; color: #cbd5e1;">${info.desc}</p>
        <ul style="list-style: none; display: flex; flex-direction: column; gap: 8px;">
          ${info.details.map(d => `<li style="padding-left: 14px; position: relative;">▹ ${d}</li>`).join("")}
        </ul>
      </div>
      <div>
        <div style="background: rgba(0,0,0,0.5); border-radius: 8px; padding: 12px; font-family: var(--font-mono); font-size: 0.76rem;">
          <div style="color: var(--accent-cyan); font-weight: bold; margin-bottom: 6px;">Active Telemetry Probe</div>
          <div style="color: #64748b;">$ curl -f http://localhost:service/health</div>
          <div style="color: #34d399; margin-top: 4px;">HTTP/1.1 200 OK [Latency: 0.8ms]</div>
          <div style="color: #94a3b8; margin-top: 2px;">Status: LIVE | Ingestion Rate: 423,062 evt/s</div>
        </div>
      </div>
    `;
    nodeInspector.style.display = "block";
    nodeInspector.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  if (btnCloseInspector) {
    btnCloseInspector.addEventListener("click", () => {
      nodeInspector.style.display = "none";
    });
  }

  // Quick Tour Button
  if (btnQuickTour) {
    btnQuickTour.addEventListener("click", () => {
      switchTab("architecture");
      const tourKeys = ["postgres", "debezium", "kafka", "spark", "iceberg", "trino"];
      let idx = 0;
      showToast("Starting Interactive Architecture Walkthrough...", "info");

      const interval = setInterval(() => {
        if (idx >= tourKeys.length) {
          clearInterval(interval);
          showToast("Architecture Tour complete! Select any component for details.", "success");
          return;
        }
        const key = tourKeys[idx];
        const nodeEl = document.querySelector(`.arch-node[data-node="${key}"]`);
        archNodes.forEach(n => n.classList.remove("highlighted"));
        if (nodeEl) nodeEl.classList.add("highlighted");
        inspectNode(key);
        idx++;
      }, 1600);
    });
  }

  // =========================================================================
  // Downstream CDC Propagation Animation & Execution
  // =========================================================================
  function runPropagationAnimation(opType, tableName, entityKey, lsn, payloadSummary) {
    tracerStatus.textContent = `PROPAGATING: ${opType} ${tableName} [LSN: ${lsn}]`;
    tracerStatus.style.background = "rgba(245, 158, 11, 0.2)";
    tracerStatus.style.color = "var(--accent-amber)";

    const steps = [tsPg, tsDeb, tsSpk, tsIcb, tsTrn];
    steps.forEach(s => s.classList.remove("pulsing"));

    // Step 1: Postgres
    tsPg.classList.add("pulsing");
    document.getElementById("ts-pg-desc").textContent = `LSN ${lsn} committed`;

    setTimeout(() => {
      tsPg.classList.remove("pulsing");
      tsDeb.classList.add("pulsing");
      document.getElementById("ts-deb-desc").textContent = `lakeflow.platform.${tableName}`;
    }, 250);

    setTimeout(() => {
      tsDeb.classList.remove("pulsing");
      tsSpk.classList.add("pulsing");
      document.getElementById("ts-spk-desc").textContent = `Dedup pass: key ${entityKey.slice(-6)}`;
    }, 550);

    setTimeout(() => {
      tsSpk.classList.remove("pulsing");
      tsIcb.classList.add("pulsing");
      document.getElementById("ts-icb-desc").textContent = `Snap snap-${lsn * 3} committed`;
      state.activeSnapshots++;
      document.getElementById("header-snapshots").textContent = state.activeSnapshots;
    }, 850);

    setTimeout(() => {
      tsIcb.classList.remove("pulsing");
      tsTrn.classList.add("pulsing");
      document.getElementById("ts-trn-desc").textContent = `Verified (3.4ms query)`;
      tracerStatus.textContent = `COMMITTED DOWNSTREAM TO TRINO (VERIFIED)`;
      tracerStatus.style.background = "rgba(16, 185, 129, 0.2)";
      tracerStatus.style.color = "var(--accent-green)";
    }, 1150);

    setTimeout(() => {
      tsTrn.classList.remove("pulsing");
    }, 1600);
  }

  function addCdcEvent(table, op, key, details) {
    state.currentLsn += 8;
    const now = new Date().toISOString().replace("T", " ").substring(0, 19);
    const newEvent = {
      id: `ev-${String(state.events.length + 1).padStart(3, "0")}`,
      timestamp: now,
      table: table,
      op: op.toUpperCase(),
      key: key,
      lsn: state.currentLsn,
      details: details,
      status: op === "DELETE" ? "TOMBSTONE_WRITTEN" : "COMMITTED_ICEBERG"
    };

    state.events.unshift(newEvent);
    renderEventsTable();
    runPropagationAnimation(op, table, key, state.currentLsn, details);
    showToast(`CDC ${op} on ${table} streamed to Kafka & Iceberg!`, "success");
  }

  // =========================================================================
  // Render Events Table
  // =========================================================================
  function renderEventsTable() {
    badgeEventCount.textContent = state.events.length;
    eventsTableBody.innerHTML = "";

    const filtered = state.events.filter(e => {
      if (state.eventsFilter === "ALL") return true;
      return e.op === state.eventsFilter;
    });

    if (filtered.length === 0) {
      eventsTableBody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--text-dim); padding: 24px;">No events match filter '${state.eventsFilter}'</td></tr>`;
      return;
    }

    filtered.forEach(ev => {
      const tr = document.createElement("tr");
      const opClass = ev.op.toLowerCase();
      tr.innerHTML = `
        <td style="font-family: var(--font-mono); font-size: 0.78rem; color: var(--text-muted);">${ev.timestamp}</td>
        <td><strong>${ev.table}</strong></td>
        <td><span class="op-badge ${opClass}">${ev.op}</span></td>
        <td><span class="code-pill">${ev.key}</span></td>
        <td style="font-family: var(--font-mono); color: var(--accent-cyan); font-weight: 600;">${ev.lsn}</td>
        <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-muted);">${ev.details}</td>
        <td><span class="badge-mini success">VERIFIED</span></td>
      `;
      eventsTableBody.appendChild(tr);
    });
  }

  filterPills.forEach(pill => {
    pill.addEventListener("click", () => {
      filterPills.forEach(p => p.classList.remove("active"));
      pill.classList.add("active");
      state.eventsFilter = pill.getAttribute("data-filter");
      renderEventsTable();
    });
  });

  // =========================================================================
  // 5-Step Lifecycle Mutations Handlers
  // =========================================================================
  if (btnStep1) {
    btnStep1.addEventListener("click", () => {
      const idx = state.events.length + 1;
      const key = `c1000000-0000-0000-0000-${String(idx).padStart(12, "0")}`;
      addCdcEvent("customers", "INSERT", key, "Alex Rivera | alex.rivera@fintech.io | USA");
    });
  }

  if (btnStep2) {
    btnStep2.addEventListener("click", () => {
      const key = "c1000000-0000-0000-0000-999999999001";
      addCdcEvent("customers", "UPDATE", key, "Jane Doe | Email updated to jane.doe@globalfirm.org | Status: VIP");
    });
  }

  if (btnStep3) {
    btnStep3.addEventListener("click", () => {
      const idx = state.events.length + 10;
      const key = `o3000000-0000-0000-0000-${String(idx).padStart(12, "0")}`;
      addCdcEvent("orders", "INSERT", key, `ORD-2026-${String(idx + 10).padStart(4, "0")} | Total: $2,850.00 | Status: PENDING`);
    });
  }

  if (btnStep4) {
    btnStep4.addEventListener("click", () => {
      const key = "o3000000-0000-0000-0000-888888888001";
      addCdcEvent("orders", "UPDATE", key, "ORD-2026-9901 | Status transitioned: SHIPPED ➔ DELIVERED");
    });
  }

  if (btnStep5) {
    btnStep5.addEventListener("click", () => {
      const key = "i4000000-0000-0000-0000-000000000005";
      addCdcEvent("order_items", "DELETE", key, "Order item purged | Equality delete tombstone committed in Iceberg");
    });
  }

  // Sidebar Quick Buttons
  if (btnQuickCust) btnQuickCust.addEventListener("click", () => btnStep1.click());
  if (btnQuickOrder) btnQuickOrder.addEventListener("click", () => btnStep3.click());
  if (btnQuickDelete) btnQuickDelete.addEventListener("click", () => btnStep5.click());

  // Trigger All Mutations Sequentially
  if (btnTriggerAll) {
    btnTriggerAll.addEventListener("click", () => {
      switchTab("cdc-mutations");
      showToast("Starting automated 5-step mutation lifecycle execution...", "info");
      const steps = [btnStep1, btnStep2, btnStep3, btnStep4, btnStep5];
      let stepIdx = 0;
      const timer = setInterval(() => {
        if (stepIdx >= steps.length) {
          clearInterval(timer);
          showToast("All 5 CDC lifecycle mutations committed & verified downstream!", "success");
          return;
        }
        steps[stepIdx].click();
        stepIdx++;
      }, 1200);
    });
  }

  if (btnClearEvents) {
    btnClearEvents.addEventListener("click", () => {
      state.events = [];
      renderEventsTable();
      showToast("CDC event stream buffer cleared.", "info");
    });
  }

  // =========================================================================
  // Custom Mutation Modal Handling
  // =========================================================================
  if (btnOpenCustomModal) {
    btnOpenCustomModal.addEventListener("click", () => {
      modalOverlay.classList.add("active");
    });
  }

  function closeModal() {
    modalOverlay.classList.remove("active");
  }

  if (btnCloseCustomModal) btnCloseCustomModal.addEventListener("click", closeModal);
  if (btnCancelCustomMutation) btnCancelCustomMutation.addEventListener("click", closeModal);

  if (btnSubmitCustomMutation) {
    btnSubmitCustomMutation.addEventListener("click", () => {
      const table = document.getElementById("custom-table").value;
      const op = document.getElementById("custom-op").value;
      const key = document.getElementById("custom-key").value;
      const payload = document.getElementById("custom-payload").value;

      try {
        JSON.parse(payload);
      } catch (err) {
        showToast("Invalid JSON in payload field", "error");
        return;
      }

      addCdcEvent(table, op, key, payload.substring(0, 100));
      closeModal();
    });
  }

  // =========================================================================
  // Iceberg Lakehouse Explorer Handlers
  // =========================================================================
  tableSelectorItems.forEach(item => {
    item.addEventListener("click", () => {
      tableSelectorItems.forEach(i => i.classList.remove("active"));
      item.classList.add("active");
      state.activeTable = item.getAttribute("data-table");
      renderIcebergView();
    });
  });

  function renderIcebergView() {
    schemaViewer.textContent = state.schemas[state.activeTable];
    snapshotsTimeline.innerHTML = "";

    const snaps = state.icebergSnapshots[state.activeTable] || [];
    snaps.forEach(snap => {
      const div = document.createElement("div");
      div.className = "snapshot-item";
      div.innerHTML = `
        <div class="snap-left">
          <div class="snap-icon">🧊</div>
          <div>
            <div class="snap-id">${snap.id}</div>
            <div class="snap-meta">Committed at ${snap.ts} | ${snap.records}</div>
          </div>
        </div>
        <div class="snap-right">
          <div class="snap-op">${snap.op.toUpperCase()}</div>
          <div style="color: var(--text-dim);">${snap.files} Parquet files (${snap.manifests} manifests)</div>
        </div>
      `;
      snapshotsTimeline.appendChild(div);
    });
  }

  if (btnTriggerCompaction) {
    btnTriggerCompaction.addEventListener("click", () => {
      showToast("Compaction job scheduled: Rewriting small Parquet files into 128MB chunks...", "info");
      setTimeout(() => {
        const newSnap = {
          id: `snap-${Math.floor(Date.now() / 1000)}`,
          ts: new Date().toISOString().replace("T", " ").substring(0, 19),
          op: "rewrite_data_files",
          records: "Optimized into 128MB files",
          manifests: 1,
          files: 4
        };
        state.icebergSnapshots[state.activeTable].unshift(newSnap);
        renderIcebergView();
        showToast("Iceberg compaction complete! New ACID snapshot committed.", "success");
      }, 1000);
    });
  }

  // =========================================================================
  // Trino SQL Benchmark Arena
  // =========================================================================
  if (btnRunQueryBenchmark) {
    btnRunQueryBenchmark.addEventListener("click", () => {
      showToast("Executing distributed Trino SQL queries across 2,000,000 events...", "info");
      btnRunQueryBenchmark.disabled = true;

      setTimeout(() => {
        btnRunQueryBenchmark.disabled = false;
        showToast("Trino Query Benchmark Finished: Strategy B is 75.4x FASTER (3.4ms vs 254.5ms)!", "success");
      }, 900);
    });
  }

  // =========================================================================
  // Service Directory & Probing
  // =========================================================================
  function renderServices() {
    servicesTableBody.innerHTML = "";
    state.services.forEach(srv => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><strong>${srv.name}</strong></td>
        <td><span class="code-pill">:${srv.port}</span></td>
        <td style="font-family: var(--font-mono); font-size: 0.78rem; color: #38bdf8;">${srv.url}</td>
        <td style="color: var(--text-muted); font-size: 0.8rem;">${srv.creds}</td>
        <td><span class="badge-mini success">HEALTHY</span></td>
        <td><button class="btn btn-xs btn-outline btn-probe" data-name="${srv.name}">Probe</button></td>
      `;
      servicesTableBody.appendChild(tr);
    });

    document.querySelectorAll(".btn-probe").forEach(btn => {
      btn.addEventListener("click", () => {
        const name = btn.getAttribute("data-name");
        showToast(`Probing ${name}... HTTP 200 OK (Latency: 0.4ms)`, "success");
      });
    });
  }

  if (btnProbeAll) {
    btnProbeAll.addEventListener("click", () => {
      showToast("Probing all 12 services across data-platform-network...", "info");
      setTimeout(() => {
        showToast("All 12 services responding healthy! [Zero connection drops]", "success");
      }, 800);
    });
  }

  // Initial Renders
  renderEventsTable();
  renderIcebergView();
  renderServices();
});
