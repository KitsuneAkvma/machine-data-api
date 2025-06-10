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

// Initialize database tables - now more flexible
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS machine_data (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      machine_id TEXT,
      device_type TEXT DEFAULT 'unknown',
      timestamp DATETIME,
      received_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      raw_payload TEXT NOT NULL,
      extracted_data TEXT DEFAULT '{}',
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

  db.run(`
    CREATE INDEX IF NOT EXISTS idx_received_at ON machine_data(received_at);
  `);
});

// Middleware stack
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// Flexible rate limiting based on identifiable machine or IP fallback
const machineRateLimiter = rateLimit({
  windowMs: 10 * 1000, // 10 seconds
  max: 1,
  keyGenerator: (req) => {
    // Try multiple ways to identify the machine
    const payload = req.body || {};
    return payload.machineId || 
           payload.machine_id || 
           payload.deviceId || 
           payload.device_id ||
           payload.id ||
           payload.serial ||
           req.ip;
  },
  message: {
    success: false,
    error: 'Rate limit exceeded. Maximum 1 request per 10 seconds per machine.',
    retryAfter: '10 seconds'
  },
  standardHeaders: true,
  legacyHeaders: false,
});

// Global fallback rate limiter
const globalLimiter = rateLimit({
  windowMs: 1 * 60 * 1000, // 1 minute
  max: 100,
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

db.all("SELECT DISTINCT machine_id FROM machine_data WHERE machine_id IS NOT NULL", (err, rows) => {
  if (!err) {
    systemMetrics.connectedDevices = new Set(rows.map(r => r.machine_id));
  }
});

// Smart data extraction utility
const extractMachineInfo = (payload) => {
  if (!payload || typeof payload !== 'object') {
    return {
      machineId: null,
      deviceType: 'unknown',
      timestamp: null,
      extractedData: {}
    };
  }

  // Smart machine ID extraction
  const machineId = payload.machineId || 
                   payload.machine_id || 
                   payload.deviceId || 
                   payload.device_id ||
                   payload.id ||
                   payload.serial ||
                   payload.name ||
                   null;

  // Smart device type extraction
  const deviceType = payload.deviceType || 
                    payload.device_type ||
                    payload.type ||
                    payload.category ||
                    'unknown';

  // Smart timestamp extraction
  let timestamp = null;
  const timestampFields = ['timestamp', 'time', 'datetime', 'created_at', 'recorded_at'];
  for (const field of timestampFields) {
    if (payload[field]) {
      const ts = new Date(payload[field]);
      if (!isNaN(ts.getTime())) {
        timestamp = ts.toISOString();
        break;
      }
    }
  }

  // Extract meaningful data (everything except our extracted fields)
  const extractedData = { ...payload };
  delete extractedData.machineId;
  delete extractedData.machine_id;
  delete extractedData.deviceId;
  delete extractedData.device_id;
  delete extractedData.deviceType;
  delete extractedData.device_type;
  delete extractedData.type;
  delete extractedData.category;

  return {
    machineId,
    deviceType,
    timestamp,
    extractedData
  };
};

// Minimal validation - just check if we have ANY data
const validateBasicPayload = (req, res, next) => {
  if (!req.body || Object.keys(req.body).length === 0) {
    return res.status(400).json({
      success: false,
      error: 'Empty payload received. Send some data.',
      hint: 'Any JSON object is acceptable - we\'ll figure out what it contains'
    });
  }

  // Check payload size (basic sanity check)
  const payloadSize = JSON.stringify(req.body).length;
  if (payloadSize > 10 * 1024 * 1024) { // 10MB limit
    return res.status(413).json({
      success: false,
      error: 'Payload too large. Maximum 10MB allowed.'
    });
  }

  next();
};

// Health check endpoint with updated guide
app.get('/health', (req, res) => {
  db.get("SELECT 1", (err) => {
    const dbStatus = err ? 'error' : 'connected';
    
    const showGuide = req.query.guide === 'true';
    
    if (showGuide) {
      res.send(`
<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>API Guide - Flexible Machine Data</title>
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.6; }
        h1 { color: #2563eb; }
        h2 { color: #1e40af; border-bottom: 2px solid #e5e7eb; padding-bottom: 8px; }
        h3 { color: #1e3a8a; }
        code { background: #f3f4f6; color: #1f2937; padding: 2px 4px; border-radius: 3px; font-size: 0.9em; }
        pre { background: #1f2937; color: #f9fafb; padding: 16px; border-radius: 8px; overflow-x: auto; }
        .success { background: #d1fae5; border-left: 4px solid #10b981; padding: 12px; margin: 16px 0; }
        .info { background: #dbeafe; border-left: 4px solid #3b82f6; padding: 12px; margin: 16px 0; }
        .step { background: #eff6ff; padding: 12px; margin: 8px 0; border-radius: 6px; }
        button { background: #2563eb; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; }
        button:hover { background: #1d4ed8; }
    </style>
</head>
<body>
    <h1>🚀 Flexible Machine Data API - Send ANY JSON!</h1>
    
    <div class="success">
        <h3>✨ New Feature: Zero Requirements!</h3>
        <p>This API now accepts <strong>ANY JSON payload</strong> from your machines. No required fields, no strict validation - just send your data and we'll intelligently extract what we can!</p>
    </div>
    
    <h2>🔧 Quick Test (Copy & Paste in Browser Console)</h2>
    
    <div class="step">
        <h3>Method 1: Simple Data</h3>
        <pre><code>fetch('${req.protocol}://${req.get('host')}/api/machine-data', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    temp: 25.6,
    status: "running",
    location: "Factory Floor A"
  })
})
.then(r => r.json())
.then(data => console.log('✅ SUCCESS:', data));</code></pre>
    </div>

    <div class="step">
        <h3>Method 2: Rich Machine Data</h3>
        <pre><code>fetch('${req.protocol}://${req.get('host')}/api/machine-data', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    machineId: "PRESS-001",
    deviceType: "hydraulic_press", 
    timestamp: new Date().toISOString(),
    pressure: 1500,
    temperature: 78.2,
    cycles_completed: 1247,
    operator: "John Smith",
    shift: "morning"
  })
})
.then(r => r.json())
.then(data => console.log('✅ SUCCESS:', data));</code></pre>
    </div>

    <div class="step">
        <h3>Method 3: Whatever Your Machine Sends</h3>
        <pre><code>// Your machine can send literally anything like this:
fetch('${req.protocol}://${req.get('host')}/api/machine-data', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    xyz_sensor_reading: 42.7,
    custom_field_name: "some_value",
    nested_object: {
      sub_field: 123,
      another_field: true
    },
    timestamp_field: "2025-06-10T14:30:00Z"
  })
})
.then(r => r.json())
.then(data => console.log('✅ SUCCESS:', data));</code></pre>
    </div>

    <h2>🎯 Smart Field Detection</h2>
    
    <div class="info">
        <p><strong>The API automatically detects:</strong></p>
        <ul>
            <li><strong>Machine ID:</strong> machineId, machine_id, deviceId, device_id, id, serial, name</li>
            <li><strong>Device Type:</strong> deviceType, device_type, type, category</li>
            <li><strong>Timestamp:</strong> timestamp, time, datetime, created_at, recorded_at</li>
            <li><strong>Data:</strong> Everything else gets stored as measurement data</li>
        </ul>
    </div>

    <h2>📊 Check Your Data</h2>
    <ul>
        <li><a href="${req.protocol}://${req.get('host')}/health" target="_blank">Dashboard</a></li>
        <li><a href="${req.protocol}://${req.get('host')}/api/machine-data" target="_blank">All Data</a></li>
        <li><a href="${req.protocol}://${req.get('host')}/api/stats" target="_blank">Statistics</a></li>
    </ul>

    <div class="info">
        <h3>⚡ Rate Limits:</h3>
        <ul>
            <li>1 request per 10 seconds per machine/IP</li>
            <li>100 requests per minute globally</li>
        </ul>
    </div>

    <script>
        document.querySelectorAll('pre').forEach(pre => {
            pre.addEventListener('click', () => {
                navigator.clipboard.writeText(pre.textContent);
                const feedback = document.createElement('div');
                feedback.textContent = '📋 Copied!';
                feedback.style.cssText = 'position: absolute; top: 10px; right: 10px; background: #10b981; color: white; padding: 4px 8px; border-radius: 4px; font-size: 12px;';
                pre.style.position = 'relative';
                pre.appendChild(feedback);
                setTimeout(() => feedback.remove(), 2000);
            });
        });
    </script>
</body>
</html>
      `);
    } else {
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
        guide: {
          available: true,
          url: `${req.protocol}://${req.get('host')}/health?guide=true`,
          description: "Flexible API - accepts any JSON payload"
        }
      });
    }
  });
});

// POST endpoint - accepts ANY JSON payload
app.post('/api/machine-data', machineRateLimiter, validateBasicPayload, (req, res) => {
  const rawPayload = req.body;
  const { machineId, deviceType, timestamp, extractedData } = extractMachineInfo(rawPayload);

  const insertSQL = `
    INSERT INTO machine_data (machine_id, device_type, timestamp, raw_payload, extracted_data, metadata)
    VALUES (?, ?, ?, ?, ?, ?)
  `;

  const params = [
    machineId,
    deviceType,
    timestamp,
    JSON.stringify(rawPayload),
    JSON.stringify(extractedData),
    JSON.stringify({
      source_ip: req.ip,
      user_agent: req.get('User-Agent'),
      content_length: req.get('Content-Length')
    })
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
    if (machineId) {
      systemMetrics.connectedDevices.add(machineId);
    }

    console.log(`Data saved: ${machineId || 'unknown'} -> Record ID: ${this.lastID}`);

    res.status(201).json({
      success: true,
      message: 'Data received and stored successfully',
      id: this.lastID,
      timestamp: new Date().toISOString(),
      extracted: {
        machineId: machineId || 'not detected',
        deviceType,
        timestamp: timestamp || 'not detected',
        dataFields: Object.keys(extractedData).length
      }
    });
  });
});

// GET endpoint - retrieve data with filtering (updated for new schema)
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

  if (machineId) {
    whereClause += ` AND machine_id = ?`;
    params.push(machineId);
  }

  if (deviceType) {
    whereClause += ` AND device_type = ?`;
    params.push(deviceType);
  }

  if (from) {
    whereClause += ` AND received_at >= ?`;
    params.push(new Date(from).toISOString());
  }

  if (to) {
    whereClause += ` AND received_at <= ?`;
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
      SELECT id, machine_id, device_type, timestamp, received_at, raw_payload, extracted_data, metadata
      FROM machine_data 
      ${whereClause}
      ORDER BY received_at DESC
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
        raw_payload: JSON.parse(row.raw_payload),
        extracted_data: JSON.parse(row.extracted_data),
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
    SELECT id, machine_id, device_type, timestamp, received_at, raw_payload, extracted_data, metadata
    FROM machine_data 
    WHERE machine_id = ?
    ORDER BY received_at DESC
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
      raw_payload: JSON.parse(row.raw_payload),
      extracted_data: JSON.parse(row.extracted_data),
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
    db.prepare("SELECT COUNT(DISTINCT machine_id) as unique_machines FROM machine_data WHERE machine_id IS NOT NULL"),
    db.prepare("SELECT DISTINCT device_type FROM machine_data"),
    db.prepare("SELECT COUNT(*) as recent FROM machine_data WHERE received_at > datetime('now', '-24 hours')"),
    db.prepare("SELECT machine_id, COUNT(*) as message_count FROM machine_data WHERE machine_id IS NOT NULL GROUP BY machine_id ORDER BY message_count DESC")
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

// Cleanup old data endpoint
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

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    path: req.path,
    method: req.method,
    availableEndpoints: [
      'GET /health',
      'POST /api/machine-data (accepts ANY JSON)',
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
  console.log(`🚀 Flexible Machine Data API running on port ${PORT}`);
  console.log(`📊 Health check: http://localhost:${PORT}/health`);
  console.log(`📡 Data endpoint: http://localhost:${PORT}/api/machine-data`);
  console.log(`✨ NEW: Accepts ANY JSON payload - no required fields!`);
  console.log(`⚡ Rate limit: 1 request per 10 seconds per machine`);
  console.log(`🌐 Global limit: 100 requests per minute`);
  console.log(`💾 Database: SQLite at ${dbPath}`);
});

module.exports = app;