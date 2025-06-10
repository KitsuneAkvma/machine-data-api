const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Database setup
const dbPath = process.env.DB_PATH || path.join(__dirname, 'machine_data.db');
const db = new sqlite3.Database(dbPath);

// Initialize database tables
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS machine_data (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      machine_id TEXT NOT NULL,
      device_type TEXT DEFAULT 'unknown',
      timestamp DATETIME NOT NULL,
      received_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      data TEXT NOT NULL,
      metadata TEXT DEFAULT '{}',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  db.run(`
    CREATE INDEX IF NOT EXISTS idx_machine_id ON machine_data(machine_id);
  `);

  db.run(`
    CREATE INDEX IF NOT EXISTS idx_timestamp ON machine_data(timestamp);
  `);

  db.run(`
    CREATE INDEX IF NOT EXISTS idx_device_type ON machine_data(device_type);
  `);
});

// Middleware stack
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// Per-machine rate limiting - 10 seconds between requests per machine
const machineRateLimiter = rateLimit({
  windowMs: 10 * 1000, // 10 seconds
  max: 1, // 1 request per 10 seconds per machine
  keyGenerator: (req) => {
    // Use machineId from body if available, fallback to IP
    return req.body?.machineId || req.ip;
  },
  message: {
    success: false,
    error: 'Rate limit exceeded. Maximum 1 request per 10 seconds per machine.',
    retryAfter: '10 seconds',
    machineId: (req) => req.body?.machineId || 'unknown'
  },
  standardHeaders: true,
  legacyHeaders: false,
});

// Global fallback rate limiter (in case of abuse)
const globalLimiter = rateLimit({
  windowMs: 1 * 60 * 1000, // 1 minute
  max: 100, // 100 requests per minute globally
  message: {
    success: false,
    error: 'Global rate limit exceeded. Try again in a minute.',
    retryAfter: '60 seconds'
  }
});

app.use('/api/', globalLimiter);

// System metrics cache
let systemMetrics = {
  totalMessages: 0,
  lastMessage: null,
  connectedDevices: new Set()
};

// Update metrics from database on startup
db.get("SELECT COUNT(*) as count FROM machine_data", (err, row) => {
  if (!err) systemMetrics.totalMessages = row.count;
});

db.all("SELECT DISTINCT machine_id FROM machine_data", (err, rows) => {
  if (!err) {
    systemMetrics.connectedDevices = new Set(rows.map(r => r.machine_id));
  }
});

// Validation middleware
const validateMachineData = (req, res, next) => {
  const { machineId, timestamp, data } = req.body;
  
  if (!machineId || !timestamp || !data) {
    return res.status(400).json({
      success: false,
      error: 'Missing required fields: machineId, timestamp, data',
      receivedFields: Object.keys(req.body)
    });
  }

  // Timestamp validation
  const ts = new Date(timestamp);
  if (isNaN(ts.getTime())) {
    return res.status(400).json({
      success: false,
      error: 'Invalid timestamp format. Use ISO 8601 format.'
    });
  }

  // Data validation
  if (typeof data !== 'object') {
    return res.status(400).json({
      success: false,
      error: 'Data field must be an object'
    });
  }

  next();
};

// Health check endpoint
app.get('/health', (req, res) => {
    // Quick DB health check
    db.get("SELECT 1", (err) => {
      const dbStatus = err ? 'error' : 'connected';
      
      // Accept query parameter to show different content
      const showGuide = req.query.guide === 'true';
      
      if (showGuide) {
        // Serve the API guide as HTML
        res.send(`
  <!DOCTYPE html>
  <html lang="pl">
  <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>API Guide - Machine Data</title>
      <style>
          body { font-family: system-ui, -apple-system, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.6; }
          h1 { color: #2563eb; }
          h2 { color: #1e40af; border-bottom: 2px solid #e5e7eb; padding-bottom: 8px; }
          h3 { color: #1e3a8a; }d1e9c9
          code { color:#d1fae5; padding: 2px 4px; border-radius: 3px; font-size: 0.9em; }
          pre { background: #1f2937; color: #f9fafb; padding: 16px; border-radius: 8px; overflow-x: auto; }
          .warning { background: #fef3c7; border-left: 4px solid #f59e0b; padding: 12px; margin: 16px 0; }
          .success { background: #d1fae5; border-left: 4px solid #10b981; padding: 12px; margin: 16px 0; }
          .error { background: #fee2e2; border-left: 4px solid #ef4444; padding: 12px; margin: 16px 0; }
          .step { background: #eff6ff; padding: 12px; margin: 8px 0; border-radius: 6px; }
          button { background: #2563eb; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; }
          button:hover { background: #1d4ed8; }
      </style>
  </head>
  <body>
      <h1>🚀 API Guide - Jak wysłać dane maszyny przez przeglądarkę</h1>
      
      <h2>🔧 Metoda 1: Testowanie przez konsolę przeglądarki (NAJŁATWIEJSZE)</h2>
      
      <div class="step">
          <h3>Krok 1: Otwórz konsolę</h3>
          <p>1. Naciśnij <strong>F12</strong> (lub kliknij prawym przyciskiem → "Zbadaj element")<br>
          2. Kliknij zakładkę <strong>"Console"</strong> (Konsola)</p>
      </div>
      
      <div class="step">
          <h3>Krok 2: Skopiuj i wklej kod</h3>
          <p>Wklej poniższy kod w konsoli i naciśnij <strong>Enter</strong>:</p>
          
          <pre><code>// Wyślij dane testowe maszyny
  fetch('${req.protocol}://${req.get('host')}/api/machine-data', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      machineId: "MASZYNA-001",
      deviceType: "sensor",
      timestamp: new Date().toISOString(),
      data: {
        temperature: 23.5,
        humidity: 45,
        pressure: 1013,
        status: "working"
      },
      metadata: {
        location: "Hala A",
        operator: "Jan Kowalski"
      }
    })
  })
  .then(response => response.json())
  .then(data => {
    console.log('✅ SUKCES! Dane wysłane:', data);
    alert('Dane wysłane pomyślnie! Sprawdź dashboard.');
  })
  .catch(error => {
    console.error('❌ BŁĄD:', error);
    alert('Błąd wysyłania: ' + error.message);
  });</code></pre>
          
          <button onclick="navigator.clipboard.writeText(document.querySelector('pre code').textContent)">
              📋 Skopiuj kod
          </button>
      </div>
      
      <h2>📊 Sprawdź swoje dane</h2>
      <ul>
          <li><a href="${req.protocol}://${req.get('host')}/health" target="_blank">Dashboard</a> - główny panel</li>
          <li><a href="${req.protocol}://${req.get('host')}/api/machine-data" target="_blank">Wszystkie dane</a></li>
          <li><a href="${req.protocol}://${req.get('host')}/api/stats" target="_blank">Statystyki</a></li>
      </ul>
      
      <h2>🎯 Przykłady danych różnych typów maszyn</h2>
      
      <h3>Bosch Rexroth:</h3>
      <pre><code>{
    "machineId": "BOSCH-REXROTH-01",
    "deviceType": "torque_sdrive", 
    "timestamp": "2025-06-10T12:00:00.000Z",
    "data": {
      "temperature": 32.3,
      "torque": 55.2,
      "vibrations": 0.02
    }
  }</code></pre>
      
      <div class="warning">
          <h3>⚠️ Ważne limity:</h3>
          <ul>
              <li><strong>1 wiadomość na 10 sekund</strong> na maszynę</li>
              <li><strong>100 wiadomości na minutę</strong> globalnie</li>
              <li>Dane starsze niż 30 dni mogą być automatycznie usuwane</li>
          </ul>
      </div>
      
      <h2>🆘 Rozwiązywanie problemów</h2>
      
      <div class="error">
          <strong>Błąd 400 - "Missing required fields":</strong><br>
          Sprawdź czy masz wszystkie wymagane pola: <code>machineId</code>, <code>timestamp</code>, <code>data</code>
      </div>
      
      <div class="error">
          <strong>Błąd 429 - "Rate limit exceeded":</strong><br>
          Czekaj 10 sekund między wysyłaniem danych z tej samej maszyny
      </div>
      
      
      <hr>
      <p><small>Potrzebujesz pomocy? Użyj konsoli przeglądarki - to najprostszy sposób!</small></p>
      
      <script>
          // Auto-copy function for code blocks
          document.querySelectorAll('pre').forEach(pre => {
              pre.style.position = 'relative';
              pre.addEventListener('click', () => {
                  navigator.clipboard.writeText(pre.textContent);
                  
                  // Show feedback
                  const feedback = document.createElement('div');
                  feedback.textContent = '📋 Skopiowane!';
                  feedback.style.cssText = 'position: absolute; top: 10px; right: 10px; background: #10b981; color: white; padding: 4px 8px; border-radius: 4px; font-size: 12px;';
                  pre.appendChild(feedback);
                  
                  setTimeout(() => feedback.remove(), 2000);
              });
          });
      </script>
  </body>
  </html>
        `);
      } else {
        // Standard health check response
        res.json({
          status: dbStatus === 'connected' ? 'operational' : 'degraded',
          database: dbStatus,
          timestamp: new Date().toISOString(),
          uptime: process.uptime(),
          memory: process.memoryUsage(),
          metrics: {
            totalMessages: systemMetrics.totalMessages,
            connectedDevices: systemMetrics.connectedDevices.size,
            lastActivity: systemMetrics.lastMessage
          },
          // Add guide access info
          guide: {
            available: true,
            url: `${req.protocol}://${req.get('host')}/health?guide=true`,
            description: "Complete API usage guide with examples"
          }
        });
      }
    });
  });

// POST endpoint - data ingestion with per-machine rate limiting
app.post('/api/machine-data', machineRateLimiter, validateMachineData, (req, res) => {
  const { machineId, deviceType = 'unknown', timestamp, data, metadata = {} } = req.body;

  const insertSQL = `
    INSERT INTO machine_data (machine_id, device_type, timestamp, data, metadata)
    VALUES (?, ?, ?, ?, ?)
  `;

  const params = [
    machineId,
    deviceType,
    new Date(timestamp).toISOString(),
    JSON.stringify(data),
    JSON.stringify(metadata)
  ];

  db.run(insertSQL, params, function(err) {
    if (err) {
      console.error('Database error:', err);
      return res.status(500).json({
        success: false,
        error: 'Database error occurred'
      });
    }

    // Update metrics
    systemMetrics.totalMessages++;
    systemMetrics.lastMessage = new Date();
    systemMetrics.connectedDevices.add(machineId);

    console.log(`Data saved: ${machineId} -> Record ID: ${this.lastID}`);

    res.status(201).json({
      success: true,
      message: 'Data received and stored',
      id: this.lastID,
      timestamp: new Date().toISOString()
    });
  });
});

// GET endpoint - retrieve data with filtering
app.get('/api/machine-data', (req, res) => {
  const { 
    machineId, 
    limit = 100, 
    offset = 0, 
    from, 
    to,
    deviceType 
  } = req.query;

  let whereClause = 'WHERE 1=1';
  let params = [];
  let paramIndex = 1;

  if (machineId) {
    whereClause += ` AND machine_id = ?`;
    params.push(machineId);
  }

  if (deviceType) {
    whereClause += ` AND device_type = ?`;
    params.push(deviceType);
  }

  if (from) {
    whereClause += ` AND timestamp >= ?`;
    params.push(new Date(from).toISOString());
  }

  if (to) {
    whereClause += ` AND timestamp <= ?`;
    params.push(new Date(to).toISOString());
  }

  // Count total records
  const countSQL = `SELECT COUNT(*) as total FROM machine_data ${whereClause}`;
  
  db.get(countSQL, params, (err, countRow) => {
    if (err) {
      return res.status(500).json({
        success: false,
        error: 'Database error occurred'
      });
    }

    // Get paginated data
    const dataSQL = `
      SELECT id, machine_id, device_type, timestamp, received_at, data, metadata
      FROM machine_data 
      ${whereClause}
      ORDER BY timestamp DESC
      LIMIT ? OFFSET ?
    `;

    const dataParams = [...params, parseInt(limit), parseInt(offset)];

    db.all(dataSQL, dataParams, (err, rows) => {
      if (err) {
        return res.status(500).json({
          success: false,
          error: 'Database error occurred'
        });
      }

      // Parse JSON fields
      const processedRows = rows.map(row => ({
        ...row,
        data: JSON.parse(row.data),
        metadata: JSON.parse(row.metadata)
      }));

      res.json({
        success: true,
        data: processedRows,
        pagination: {
          total: countRow.total,
          limit: parseInt(limit),
          offset: parseInt(offset),
          hasMore: (parseInt(offset) + parseInt(limit)) < countRow.total
        }
      });
    });
  });
});

// GET specific machine data
app.get('/api/machine-data/:machineId', (req, res) => {
  const { machineId } = req.params;
  const { limit = 50 } = req.query;

  const sql = `
    SELECT id, machine_id, device_type, timestamp, received_at, data, metadata
    FROM machine_data 
    WHERE machine_id = ?
    ORDER BY timestamp DESC
    LIMIT ?
  `;

  db.all(sql, [machineId, parseInt(limit)], (err, rows) => {
    if (err) {
      return res.status(500).json({
        success: false,
        error: 'Database error occurred'
      });
    }

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: `No data found for machine: ${machineId}`
      });
    }

    const processedRows = rows.map(row => ({
      ...row,
      data: JSON.parse(row.data),
      metadata: JSON.parse(row.metadata)
    }));

    res.json({
      success: true,
      machineId,
      recordCount: processedRows.length,
      data: processedRows
    });
  });
});

// GET system statistics
app.get('/api/stats', (req, res) => {
  const queries = [
    db.prepare("SELECT COUNT(*) as total FROM machine_data"),
    db.prepare("SELECT COUNT(DISTINCT machine_id) as unique_machines FROM machine_data"),
    db.prepare("SELECT DISTINCT device_type FROM machine_data"),
    db.prepare("SELECT COUNT(*) as recent FROM machine_data WHERE received_at > datetime('now', '-24 hours')"),
    db.prepare("SELECT machine_id, COUNT(*) as message_count FROM machine_data GROUP BY machine_id ORDER BY message_count DESC")
  ];

  Promise.all([
    new Promise((resolve, reject) => {
      queries[0].get((err, row) => err ? reject(err) : resolve(row));
    }),
    new Promise((resolve, reject) => {
      queries[1].get((err, row) => err ? reject(err) : resolve(row));
    }),
    new Promise((resolve, reject) => {
      queries[2].all((err, rows) => err ? reject(err) : resolve(rows));
    }),
    new Promise((resolve, reject) => {
      queries[3].get((err, row) => err ? reject(err) : resolve(row));
    }),
    new Promise((resolve, reject) => {
      queries[4].all((err, rows) => err ? reject(err) : resolve(rows));
    })
  ]).then(([total, uniqueMachines, deviceTypes, recent, machineStats]) => {
    res.json({
      success: true,
      statistics: {
        totalMessages: total.total,
        uniqueMachines: uniqueMachines.unique_machines,
        deviceTypes: deviceTypes.map(dt => dt.device_type),
        recentActivity24h: recent.recent,
        lastMessage: systemMetrics.lastMessage,
        topMachines: machineStats.slice(0, 10)
      }
    });
  }).catch(err => {
    res.status(500).json({
      success: false,
      error: 'Database error occurred'
    });
  }).finally(() => {
    queries.forEach(q => q.finalize());
  });
});

// Cleanup old data endpoint (optional - for maintenance)
app.delete('/api/cleanup', (req, res) => {
  const { days = 30 } = req.query;
  
  const sql = `DELETE FROM machine_data WHERE received_at < datetime('now', '-${parseInt(days)} days')`;
  
  db.run(sql, function(err) {
    if (err) {
      return res.status(500).json({
        success: false,
        error: 'Database error occurred'
      });
    }

    res.json({
      success: true,
      message: `Cleaned up records older than ${days} days`,
      deletedRecords: this.changes
    });
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    success: false,
    error: 'Something went wrong on our end'
  });
});

// 404 handler - fixed route pattern
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    path: req.path,
    method: req.method,
    availableEndpoints: [
      'GET /health',
      'POST /api/machine-data',
      'GET /api/machine-data',
      'GET /api/machine-data/:machineId',
      'GET /api/stats',
      'DELETE /api/cleanup'
    ]
  });
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Shutting down gracefully...');
  db.close((err) => {
    if (err) {
      console.error(err.message);
    }
    console.log('Database connection closed.');
    process.exit(0);
  });
});

// Start the server
app.listen(PORT, () => {
  console.log(`🚀 Machine Data API running on port ${PORT}`);
  console.log(`📊 Health check: http://localhost:${PORT}/health`);
  console.log(`📡 Data endpoint: http://localhost:${PORT}/api/machine-data`);
  console.log(`⚡ Rate limit: 1 request per 10 seconds per machine`);
  console.log(`🌐 Global limit: 100 requests per minute`);
  console.log(`💾 Database: SQLite at ${dbPath}`);
});

module.exports = app;