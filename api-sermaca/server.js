// server.js
'use strict';

const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');
const http = require('http');
const { Server } = require('socket.io');
// Import correcto del paquete: exporta una funciÃƒÂ³n
const stringify = require('csv-stringify');
const path = require('path');

const app = express();
const server = http.createServer(app);
const port = process.env.PORT || 3000;

const API_PREFIX = '/api_aguilera';
const VZLA_TZ = 'America/Caracas';
const VZLA_LOCALE = 'es-VE';
const SOCKET_PATH = process.env.SOCKET_IO_PATH || (API_PREFIX + '/socket.io');

function parsePositiveInt(rawValue, fallbackValue) {
  const parsed = parseInt(rawValue, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallbackValue;
}

const socketTransports = [];
if (process.env.SOCKET_ALLOW_WEBSOCKET !== '0') socketTransports.push('websocket');
if (process.env.SOCKET_ALLOW_POLLING !== '0') socketTransports.push('polling');
if (socketTransports.length === 0) socketTransports.push('polling');

const io = new Server(server, {
  path: SOCKET_PATH,
  transports: socketTransports,
  serveClient: false,
  perMessageDeflate: false,
  allowEIO3: true,
  pingInterval: parsePositiveInt(process.env.SOCKET_PING_INTERVAL, 25000),
  pingTimeout: parsePositiveInt(process.env.SOCKET_PING_TIMEOUT, 30000),
  maxHttpBufferSize: parsePositiveInt(process.env.SOCKET_MAX_HTTP_BUFFER, 1000000),
  cors: {
    origin: process.env.SOCKET_CORS_ORIGIN || '*',
    methods: ['GET', 'POST'],
    credentials: false
  }
});

app.set('trust proxy', 1);
app.disable('x-powered-by');

// Middleware - CORS configurado para permitir cualquier origen
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
  credentials: false
}));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ConfiguraciÃƒÂ³n de MySQL
const pool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'hmi_data',
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Utilidad para default numÃƒÂ©rico sin usar ?? (compatibilidad amplia)
function defNum(v) {
  return (v === null || v === undefined) ? 0 : v;
}

function sanitizeRoomToken(value) {
  return String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, '')
    .slice(0, 80);
}

function mapMeasurementForRealtime(record) {
  if (!record) return null;

  return {
    timestamp_hmi: record.timestamp_hmi instanceof Date
      ? record.timestamp_hmi.toISOString()
      : record.timestamp_hmi,
    pozo: record.pozo || null,
    qm_liq: record.qm_liq,
    qm_gas: record.qm_gas,
    total_liq: record.total_liq,
    total_gas: record.total_gas,
    densidad_liq: record.densidad_liq,
    densidad_gas: record.densidad_gas,
    temp_liq: record.temp_liq,
    temp_gas: record.temp_gas,
    pres_f_liq: record.pres_f_liq,
    pres_f_gas: record.pres_f_gas,
    bsw_lab: record.bsw_lab,
    presion_cabezal: record.presion_cabezal,
    presion_casing: record.presion_casing,
    presion_linea: record.presion_linea,
    presion_macolla: record.presion_macolla,
    inv_liq: record.inv_liq,
    inv_gas: record.inv_gas,
    diluente: record.diluente
  };
}

function emitRealtimeNewMeasurements(records, insertedRows) {
  if (!Array.isArray(records) || records.length === 0) return;

  const latestRecord = records[records.length - 1];
  const payload = {
    inserted_rows: insertedRows,
    batch_size: records.length,
    server_time: new Date().toISOString(),
    latest: mapMeasurementForRealtime(latestRecord)
  };

  io.emit('realtime:mediciones', payload);

  const pozoToken = sanitizeRoomToken(latestRecord.pozo);
  if (pozoToken) {
    io.to('pozo:' + pozoToken).emit('realtime:mediciones:pozo', payload);
  }
}

io.on('connection', function (socket) {
  socket.emit('realtime:connected', {
    socket_id: socket.id,
    server_time: new Date().toISOString(),
    socket_path: SOCKET_PATH,
    transports: socketTransports
  });

  socket.on('realtime:subscribe', function (payload, ack) {
    const pozoRaw = (typeof payload === 'string')
      ? payload
      : payload && payload.pozo;
    const pozoToken = sanitizeRoomToken(pozoRaw);

    if (!pozoToken) {
      if (typeof ack === 'function') ack({ ok: false, error: 'pozo invalido' });
      return;
    }

    const roomName = 'pozo:' + pozoToken;
    socket.join(roomName);
    if (typeof ack === 'function') ack({ ok: true, room: roomName });
  });

  socket.on('realtime:unsubscribe', function (payload, ack) {
    const pozoRaw = (typeof payload === 'string')
      ? payload
      : payload && payload.pozo;
    const pozoToken = sanitizeRoomToken(pozoRaw);

    if (!pozoToken) {
      if (typeof ack === 'function') ack({ ok: false, error: 'pozo invalido' });
      return;
    }

    const roomName = 'pozo:' + pozoToken;
    socket.leave(roomName);
    if (typeof ack === 'function') ack({ ok: true, room: roomName });
  });
});

app.post(API_PREFIX + '/api/datos', async (req, res) => {
  try {
    let payload = req.body;
    if (!Array.isArray(payload)) payload = [payload];

    const defNum = (v) => v === null || v === undefined || isNaN(parseFloat(v)) ? null : parseFloat(v);

    const records = payload.map((body) => {
      return {
        timestamp_hmi: body.timestamp ? new Date(body.timestamp) : new Date(),
        pozo: (body.pozo_1 || body.pozo || '').toString().trim(),
        qm_liq: defNum(body.qm_liq_1),
        qm_gas: defNum(body.qm_gas_1),
        total_liq: defNum(body.total_liq_1),
        total_gas: defNum(body.total_gas_1),
        densidad_liq: defNum(body.densidad_liq_1),
        densidad_gas: defNum(body.densidad_gas_1),
        densidad_lab: defNum(body.densidad_lab_1),
        temp_liq: defNum(body.temp_liq_1),
        temp_gas: defNum(body.temp_gas_1),
        mA_pres_f_liq: defNum(body.mA_pres_f_liq_1),
        pres_f_liq: defNum(body.pres_f_liq_1),
        mA_pres_f_gas: defNum(body.mA_pres_f_gas_1),
        pres_f_gas: defNum(body.pres_f_gas_1),
        driv_gain_liq: defNum(body.driv_gain_liq_1),
        driv_gain_gas: defNum(body.driv_gain_gas_1),
        apr_valv: defNum(body.apr_valv_1),
        sept_valv: defNum(body.sept_valv_1),
        h2o_d: defNum(body.h2o_d_1),       // Nueva
        h2o_f: defNum(body.h2o_f_1),       // Nueva
        bsw_lab: defNum(body.bsw_lab_1),
        presion_cabezal: defNum(body.presion_cabezal_1),
        presion_casing: defNum(body.presion_casing_1),
        presion_linea: defNum(body.presion_linea_1),
        presion_macolla: defNum(body.presion_macolla_1), // Nueva
        inv_liq: defNum(body.inv_liq_1),
        inv_gas: defNum(body.inv_gas_1),
        ivo_liq_kg: defNum(body.ivo_liq_kg_1),
        ivo_gas_kg: defNum(body.ivo_gas_kg_1),
        diluente: defNum(body.diluente_1),
        vdf_vel: defNum(body.vdf_vel_1),   // Nueva
        vdf_tor: defNum(body.vdf_tor_1),   // Nueva
        vdf_amp: defNum(body.vdf_amp_1),   // Nueva
        vdf_cons: defNum(body.vdf_cons_1)  // Nueva
      };
    });

    const query = `
      INSERT INTO mediciones (
        timestamp_hmi, pozo, qm_liq, qm_gas, total_liq, total_gas,
        densidad_liq, densidad_gas, densidad_lab, temp_liq, temp_gas,
        mA_pres_f_liq, pres_f_liq, mA_pres_f_gas, pres_f_gas,
        driv_gain_liq, driv_gain_gas, apr_valv, sept_valv,
        h2o_d, h2o_f, bsw_lab,
        presion_cabezal, presion_casing, presion_linea, presion_macolla,
        inv_liq, inv_gas, ivo_liq_kg, ivo_gas_kg, diluente,
        vdf_vel, vdf_tor, vdf_amp, vdf_cons
      ) VALUES ?
    `;

    const values = records.map((r) => [
      r.timestamp_hmi, r.pozo, r.qm_liq, r.qm_gas, r.total_liq, r.total_gas,
      r.densidad_liq, r.densidad_gas, r.densidad_lab, r.temp_liq, r.temp_gas,
      r.mA_pres_f_liq, r.pres_f_liq, r.mA_pres_f_gas, r.pres_f_gas,
      r.driv_gain_liq, r.driv_gain_gas, r.apr_valv, r.sept_valv,
      r.h2o_d, r.h2o_f, r.bsw_lab,
      r.presion_cabezal, r.presion_casing, r.presion_linea, r.presion_macolla,
      r.inv_liq, r.inv_gas, r.ivo_liq_kg, r.ivo_gas_kg, r.diluente,
      r.vdf_vel, r.vdf_tor, r.vdf_amp, r.vdf_cons
    ]);

    const [result] = await pool.query(query, [values]);
    res.status(201).json({ message: 'Datos HMI almacenados', registros_insertados: result.affectedRows });

    setImmediate(function () {
      try {
        emitRealtimeNewMeasurements(records, result.affectedRows);
      } catch (socketError) {
        console.error('Error emitiendo evento realtime:', socketError);
      }
    });

  } catch (error) {
    console.error('Ã¢ÂÅ’ Error:', error);
    res.status(500).json({ error: 'Error interno' });
  }
});


app.get(API_PREFIX + '/api/databasefluxcy', async (req, res) => {
  try {
    const from = req.query.from || new Date(Date.now() - 24 * 3600000).toISOString();
    const to = req.query.to || new Date().toISOString();

const query = `
  SELECT 
    -- Truncamos a nivel de minuto (segundos = 00)
    UNIX_TIMESTAMP(
      DATE_FORMAT(timestamp_hmi, '%Y-%m-%d %H:%i:00')
    ) * 1000 AS time,
    
    -- Formato legible para tablas o depuraciÃƒÂ³n
    DATE_FORMAT(timestamp_hmi, '%m-%d %H:%i') AS timestamp_short,
    
    pozo, 
    AVG(qm_liq) AS qm_liq, 
    AVG(qm_gas) AS qm_gas, 
    MAX(total_liq) AS total_liq, 
    MAX(total_gas) AS total_gas, 
    AVG(densidad_liq) AS densidad_liq, 
    AVG(densidad_gas) AS densidad_gas, 
    AVG(densidad_lab) AS densidad_lab,
    AVG(temp_liq) AS temp_liq, 
    AVG(temp_gas) AS temp_gas, 
    AVG(pres_f_liq) AS pres_f_liq, 
    AVG(pres_f_gas) AS pres_f_gas,
    AVG(apr_valv) AS apr_valv, 
    AVG(sept_valv) AS sept_valv,
    AVG(bsw_lab) AS bsw_lab, 
    AVG(h2o_d) AS h2o_d,
    AVG(h2o_f) AS h2o_f,
    AVG(presion_cabezal) AS presion_cabezal, 
    AVG(presion_casing) AS presion_casing,
    AVG(presion_linea) AS presion_linea, 
    AVG(presion_macolla) AS presion_macolla,
    MAX(inv_liq) AS inv_liq,
    MAX(inv_gas) AS inv_gas,
    MAX(ivo_liq_kg) AS ivo_liq_kg,
    MAX(ivo_gas_kg) AS ivo_gas_kg,
    AVG(diluente) AS diluente,
    AVG(vdf_vel) AS vdf_vel,
    AVG(vdf_tor) AS vdf_tor,
    AVG(vdf_amp) AS vdf_amp,
    AVG(vdf_cons) AS vdf_cons
  FROM mediciones 
  WHERE timestamp_hmi BETWEEN ? AND ?
  
  -- Agrupamos por bloques de 5 minutos
  GROUP BY FLOOR(UNIX_TIMESTAMP(timestamp_hmi) / 300), pozo
  
  ORDER BY timestamp_hmi DESC 
  LIMIT 2000
`;


    const [rows] = await pool.query(query, [from, to]);

    res.status(200).json(rows);

  } catch (error) {
    console.error('Ã¢ÂÅ’ Error en databasefluxcy:', error);
    res.status(500).json({ error: 'Error al obtener datos' });
  }
});


function smoothSeriesByTime(rows, field, windowMs) {
  const result = [];
  const buffer = [];

  for (const row of rows) {
    const t = row.time;
    const v = row[field];

    if (v == null) {
      result.push({ ...row, [field]: null });
      continue;
    }

    buffer.push({ t, v });

    // limpiar ventana
    while (buffer.length && t - buffer[0].t > windowMs) {
      buffer.shift();
    }

    const avg =
      buffer.reduce((sum, x) => sum + x.v, 0) / buffer.length;

    result.push({ ...row, [field]: avg });
  }

  return result;
}


function aggregateProduction(rows, stepMs) {
  const result = [];
  let window = [];
  let windowStart = rows[0].time;

  for (const row of rows) {
    if (window.length === 0) {
      windowStart = row.time;
    }

    window.push(row);

    if (row.time - windowStart >= stepMs) {
      calcularVentana(window, result);

      // Ã°Å¸â€Â Mantener ÃƒÂºltimo punto como inicio de la siguiente ventana
      window = [row];
      windowStart = row.time;
    }
  }

  // Ã¢Å“â€¦ Procesar ÃƒÂºltima ventana pendiente
  if (window.length >= 2) {
    calcularVentana(window, result);
  }

  return result;
}

// Ã°Å¸â€Â§ CÃƒÂ¡lculo seguro de producciÃƒÂ³n
function calcularVentana(window, result) {
  const first = window[0];
  const last = window[window.length - 1];

  const deltaTimeH =
    (last.time - first.time) / 3600000;

  if (deltaTimeH <= 0) return;

  const prodLiqH =
    (last.total_liq - first.total_liq) / deltaTimeH;

  const prodGasH =
    (last.total_gas - first.total_gas) / deltaTimeH;

  result.push({
    time: last.time,
    prod_liq_h: prodLiqH,
    prod_gas_h: prodGasH
  });
}


app.get(API_PREFIX + '/api/produccion', async (req, res) => {
  try {
    const from = req.query.from;
    const to = req.query.to;
    let stepMin = parseInt(req.query.stepMin, 10);

    if (![1, 10, 30].includes(stepMin)) stepMin = 1;
    const stepMs = stepMin * 60000;

    const fromTime = from ? new Date(from) : new Date(Date.now() - (24 * 3600000));
    const toTime = to ? new Date(to) : new Date();

    const query = `
      SELECT
        UNIX_TIMESTAMP(CONVERT_TZ(timestamp_hmi, '+00:00', '-01:00')) * 1000 AS time,
        total_liq,
        total_gas
      FROM mediciones
      WHERE timestamp_hmi BETWEEN
        CONVERT_TZ(?, '-01:00', '+00:00')
        AND CONVERT_TZ(?, '-01:00', '+00:00')
      ORDER BY timestamp_hmi ASC
    `;
    
    const [rows] = await pool.execute(query, [fromTime, toTime]);

    if (!rows || rows.length === 0) {
      return res.json({ series: [] });
    }

    const series = [];

    // 1. MAPEAMOS LOS DATOS REALES REGISTRADOS
    // Esto garantiza que si el HMI dice 115 Bls, el grÃƒÂ¡fico muestre 115 Bls.
    rows.forEach(row => {
      series.push({
        time: row.time,
        liq_acum: Number(Number(row.total_liq).toFixed(2)),
        gas_acum: Number(Number(row.total_gas).toFixed(2)),
        tipo: 'real'
      });
    });

    // 2. PROYECCIÃƒâ€œN HACIA EL FUTURO
    // Calculamos la tendencia basada en el ÃƒÂºltimo y primer registro para proyectar
    if (rows.length >= 2) {
      const first = rows[0];
      const last = rows[rows.length - 1];
      
      const deltaMsTotal = last.time - first.time;
      const deltaLiqTotal = Number(last.total_liq) - Number(first.total_liq);
      const deltaGasTotal = Number(last.total_gas) - Number(first.total_gas);

      // Tasa de producciÃƒÂ³n por milisegundo
      const tasaLiqMs = deltaMsTotal > 0 ? deltaLiqTotal / deltaMsTotal : 0;
      const tasaGasMs = deltaMsTotal > 0 ? deltaGasTotal / deltaMsTotal : 0;

      let currentLiq = Number(last.total_liq);
      let currentGas = Number(last.total_gas);
      let projTime = last.time + stepMs;

      // Proyectamos desde el ÃƒÂºltimo dato real hasta la hora de corte (toTime)
      while (projTime <= toTime.getTime()) {
        currentLiq += (tasaLiqMs * stepMs);
        currentGas += (tasaGasMs * stepMs);

        series.push({
          time: projTime,
          liq_acum: Number(currentLiq.toFixed(2)),
          gas_acum: Number(currentGas.toFixed(2)),
          tipo: 'proyectado'
        });
        projTime += stepMs;
      }
    }

    res.json({ series });

  } catch (error) {
    console.error('Ã¢ÂÅ’ Error:', error);
    res.status(500).json({ error: 'Error en cÃƒÂ¡lculo', message: error.message });
  }
});


app.get(API_PREFIX + '/api/datos/corrida-pozo', async (req, res) => {
  try {
    const { fechaInicio, fechaFin } = req.query;
    if (!fechaInicio || !fechaFin) return res.status(400).json({ error: 'Faltan fechas' });
    const fromTime = new Date(fechaInicio);
    const toTime = new Date(fechaFin);

    if (Number.isNaN(fromTime.getTime()) || Number.isNaN(toTime.getTime())) {
      return res.status(400).json({ error: 'Formato de fecha invalido. Usa YYYY-MM-DD HH:mm:ss' });
    }

    if (fromTime > toTime) {
      return res.status(400).json({ error: 'fechaInicio no puede ser mayor a fechaFin' });
    }

    const query = `
      WITH muestras_20min AS (
        SELECT
          FLOOR(UNIX_TIMESTAMP(timestamp_hmi) / 1200) AS bucket_20min,
          timestamp_hmi,
          inv_liq,
          inv_gas,
          bsw_lab,
          temp_liq,
          presion_cabezal,
          presion_casing,
          pres_f_liq,
          diluente,
          ROW_NUMBER() OVER (
            PARTITION BY FLOOR(UNIX_TIMESTAMP(timestamp_hmi) / 1200)
            ORDER BY timestamp_hmi DESC
          ) AS rn
        FROM mediciones
        WHERE timestamp_hmi BETWEEN ? AND ?
      )
      SELECT
        DATE_FORMAT(FROM_UNIXTIME(bucket_20min * 1200), '%Y-%m-%d %H:%i:%s') AS fecha_hora,
        COALESCE(inv_liq, 0) AS inv_liq,
        COALESCE(inv_gas, 0) AS inv_gas,
        COALESCE(bsw_lab, 0) AS bsw_lab,
        COALESCE(temp_liq, 0) AS temp_liq,
        COALESCE(presion_cabezal, 0) AS presion_cabezal,
        COALESCE(presion_casing, 0) AS presion_casing,
        COALESCE(pres_f_liq, 0) AS pres_f_liq,
        COALESCE(diluente, 0) AS diluente
      FROM muestras_20min
      WHERE rn = 1
      ORDER BY bucket_20min ASC
    `;

    const [rows] = await pool.execute(query, [fechaInicio, fechaFin]);
    if (!rows.length) return res.status(200).send('No hay datos');

    const formatNum = (value) => {
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) return '0';
      return parsed.toFixed(4);
    };

    const data = rows.map((row) => ({
      fecha_hora: row.fecha_hora,
      inv_liq: formatNum(row.inv_liq),
      inv_gas: formatNum(row.inv_gas),
      bsw_lab: formatNum(row.bsw_lab),
      temp_liq: formatNum(row.temp_liq),
      presion_cabezal: formatNum(row.presion_cabezal),
      presion_casing: formatNum(row.presion_casing),
      pres_f_liq: formatNum(row.pres_f_liq),
      diluente: formatNum(row.diluente)
    }));

    const csvContent = [
      Object.keys(data[0]).join(','),
      ...data.map((d) => Object.values(d).join(','))
    ].join('\n');

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="corrida_pozo_20min.csv"');
    res.send(csvContent);

  } catch (err) { res.status(500).send(err.message); }
});


app.get(API_PREFIX + '/api/realtime/bootstrap', async (req, res) => {
  try {
    const protocolFromProxy = (req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
    const protocol = protocolFromProxy || req.protocol || 'http';
    const host = req.get('host');
    const baseUrl = protocol + '://' + host;

    let latest = null;
    let latestError = null;

    try {
      const [rows] = await pool.execute('SELECT * FROM mediciones ORDER BY timestamp_hmi DESC LIMIT 1');
      latest = rows.length ? mapMeasurementForRealtime(rows[0]) : null;
    } catch (dbError) {
      latestError = dbError.message;
    }

    res.json({
      status: 'OK',
      server_time: new Date().toISOString(),
      socket: {
        origin: baseUrl,
        path: SOCKET_PATH,
        url: baseUrl + SOCKET_PATH,
        transports: socketTransports,
        events: {
          connected: 'realtime:connected',
          subscribe: 'realtime:subscribe',
          unsubscribe: 'realtime:unsubscribe',
          stream_all: 'realtime:mediciones',
          stream_pozo: 'realtime:mediciones:pozo'
        }
      },
      latest,
      latest_error: latestError
    });

  } catch (error) {
    console.error('Error en /api/realtime/bootstrap:', error);
    res.status(500).json({
      error: 'Error interno al generar bootstrap realtime',
      details: error.message
    });
  }
});




// Health check
app.get(API_PREFIX + '/health', function (req, res) {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Status para Lua
app.get(API_PREFIX + '/api/status', function (req, res) {
  res.json({ message: 'API OK' });
});

app.get(API_PREFIX + '/api/realtime/status', function (req, res) {
  res.json({
    status: 'OK',
    socket_path: SOCKET_PATH,
    transports: socketTransports,
    clients: io.engine.clientsCount
  });
});

// ÃƒÅ¡ltimo registro (en vivo)
app.get(API_PREFIX + '/api/datos/ultimo', async (req, res) => {
  try {
    const exec = await pool.execute('SELECT * FROM mediciones ORDER BY fecha_creacion DESC LIMIT 1');
    const rows = exec[0];
    if (!rows.length) {
      res.status(404).json({ message: 'Sin datos' });
      return;
    }
    res.json(rows[0]);
  } catch (error) {
    console.error('Error al consultar ÃƒÂºltimo dato:', error);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

// Test database connection endpoint
app.get(API_PREFIX + '/api/test-db', async (req, res) => {
  const acceptsHtml = req.headers.accept && req.headers.accept.indexOf('text/html') !== -1;

  try {
    const exec = await pool.execute("SELECT NOW() as 'current_time'");
    const result = exec[0];

    if (acceptsHtml) {
      res.send(
        '<html>' +
          '<head>' +
            '<title>Database Test</title>' +
            '<style>' +
              'body { font-family: Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background-color: #f0f0f0; }' +
              '.message { padding: 20px; border-radius: 10px; text-align: center; font-size: 24px; font-weight: bold; }' +
              '.success { background-color: #d4edda; color: #155724; border: 2px solid #c3e6cb; }' +
              '.time { font-size: 16px; margin-top: 10px; font-weight: normal; }' +
            '</style>' +
          '</head>' +
          '<body>' +
            '<div class="message success">' +
              'Ã¢Å“â€¦ Database connected successfully' +
              '<div class="time">Connected at: ' + result[0].current_time + '</div>' +
            '</div>' +
          '</body>' +
        '</html>'
      );
    } else {
      res.json({
        status: 'Database connected successfully',
        time: result[0].current_time
      });
    }
  } catch (error) {
    console.error('Database connection error:', error);

    if (acceptsHtml) {
      res.status(500).send(
        '<html>' +
          '<head>' +
            '<title>Database Test</title>' +
            '<style>' +
              'body { font-family: Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background-color: #f0f0f0; }' +
              '.message { padding: 20px; border-radius: 10px; text-align: center; font-size: 24px; font-weight: bold; }' +
              '.error { background-color: #f8d7da; color: #721c24; border: 2px solid #f5c6cb; }' +
              '.details { font-size: 16px; margin-top: 10px; font-weight: normal; }' +
            '</style>' +
          '</head>' +
          '<body>' +
            '<div class="message error">' +
              'Ã¢ÂÅ’ Database connection failed' +
              '<div class="details">Error: ' + error.message + '</div>' +
            '</div>' +
          '</body>' +
        '</html>'
      );
    } else {
      res.status(500).json({
        error: 'Database connection failed',
        details: error.message
      });
    }
  }
});


// ================================================
// Ã°Å¸â€Â¹ FunciÃƒÂ³n de suavizado EMA para mÃƒÂºltiples columnas
function smoothEMA2(series, keys, alpha = 0.2) {
  // Inicializamos el valor previo para cada key
  const prevValues = {};
  keys.forEach(key => {
    prevValues[key] = series[0][key];
  });

  return series.map((row, i) => {
    if (i === 0) return {...row}; // primer valor sin cambio
    const newRow = {...row};
    keys.forEach(key => {
      newRow[key] = alpha * row[key] + (1 - alpha) * prevValues[key];
      prevValues[key] = newRow[key];
    });
    return newRow;
  });
}


// Ã¢Å“â€¦ Endpoint con LÃƒÂ³gica de Suavizado y Filtro de Outliers - Sin LÃƒÂ­mite
app.get(API_PREFIX + '/api/qm', async (req, res) => {
  try {
    const from = req.query.from;
    const to = req.query.to;
    // Se elimina el uso de maxRows

    // ParÃƒÂ¡metros de suavizado
    const smooth = req.query.smooth === '1';
    const alpha = parseFloat(req.query.alpha || '0.05'); 

    const fromTime = from ? new Date(from) : new Date(Date.now() - 3600000);
    const toTime = to ? new Date(to) : new Date();

    // Query modificada: Se eliminÃƒÂ³ LIMIT y el parÃƒÂ¡metro ? correspondiente
    const query = `
      SELECT 
        UNIX_TIMESTAMP(CONVERT_TZ(timestamp_hmi, '+00:00', '-01:00')) * 1000 AS time,
        qm_liq,
        qm_gas
      FROM mediciones
      WHERE timestamp_hmi BETWEEN ? AND ?
      ORDER BY timestamp_hmi ASC
    `;

    // EjecuciÃƒÂ³n de la consulta sin el parÃƒÂ¡metro maxRows
    const [rows] = await pool.execute(query, [fromTime, toTime]);

    let series = rows;

    // Ã°Å¸â€Â¥ LÃƒÂ³gica de suavizado (se mantiene igual, pero procesa todas las filas)
    if (smooth && series.length > 0) {
      const keys = ['qm_liq', 'qm_gas'];
      let lastValidValues = {};
      
      keys.forEach(key => {
        lastValidValues[key] = Math.max(0, series[0][key] || 0);
      });

      series = series.map((row) => {
        keys.forEach(key => {
          let currentValue = row[key];

          if (currentValue < 0 || currentValue === null || currentValue === undefined) {
            currentValue = lastValidValues[key];
          }

          const smoothedValue = (currentValue * alpha) + (lastValidValues[key] * (1 - alpha));
          lastValidValues[key] = smoothedValue;
          row[key] = Math.round(smoothedValue * 10000) / 10000;
        });
        return row;
      });
    }

    res.json({
      meta: {
        smooth,
        alpha: smooth ? alpha : null,
        total_records: series.length,
        timezone: '-01:00'
      },
      series
    });

  } catch (error) {
    console.error('Ã¢ÂÅ’ Error en /api/qm:', error);
    res.status(500).json({
      error: 'Error interno al consultar qm',
      details: error.message
    });
  }
});


// Ã¢Å“â€¦ Endpoint optimizado para Grafana Infinity - Densidad (Sin lÃƒÂ­mite)
app.get(API_PREFIX + '/api/rho', async (req, res) => {
  try {
    const from = req.query.from;
    const to = req.query.to;

    // Convertir a Date vÃƒÂ¡lidas
    const fromTime = from ? new Date(from) : new Date(Date.now() - 3600000);
    const toTime = to ? new Date(to) : new Date();

    const diffHours = Math.abs(toTime - fromTime) / 36e5;

    const columns = `
      UNIX_TIMESTAMP(CONVERT_TZ(timestamp_hmi, '+00:00', '-01:00')) * 1000 AS time,
      DATE_FORMAT(timestamp_hmi, '%Y-%m-%dT%H:%i:%sZ') AS fecha_creacion_iso,
      densidad_liq AS rho_liq,
      densidad_gas AS rho_gas
    `;

    // Se eliminÃƒÂ³ 'LIMIT ?' para traer todos los registros del rango
    const query = `
      SELECT ${columns}
      FROM mediciones
      WHERE timestamp_hmi BETWEEN ? AND ?
      ORDER BY timestamp_hmi ASC
    `;

    // Se ejecuta la consulta solo con los parÃƒÂ¡metros de tiempo
    const [rows] = await pool.execute(query, [fromTime, toTime]);

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        rango_horas: diffHours.toFixed(2),
        total: rows.length,
        desde: fromTime.toISOString(),
        hasta: toTime.toISOString()
      },
      series: rows
    });

  } catch (error) {
    console.error('Error en /api/rho:', error);
    res.status(500).json({
      error: 'Error interno al consultar rho',
      details: error.message
    });
  }
});


// Endpoint optimizado para Grafana Infinity - clockmeter
app.get(API_PREFIX + '/api/total', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time,
        total_liq AS totalliq,
        total_gas AS totalgas,
        inv_liq AS vliq,
        inv_gas AS vgas
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;

    const [rows] = await pool.execute(query);

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        tipo: 'clockmeter',
        total: rows.length,
        descripcion: 'ÃƒÅ¡ltima lectura'
      },
      data: rows
    });
  } catch (error) {
    console.error('Error en /api/total:', error);
    res.status(500).json({ error: 'Error interno al consultar', details: error.message });
  }
});


// Ã¢Å“â€¦ Endpoint optimizado para Grafana Infinity - Fase Gaseosa (Sin lÃƒÂ­mite)
app.get(API_PREFIX + '/api/vp', async (req, res) => {
  try {
    const from = req.query.from;
    const to = req.query.to;

    const fromTime = from ? new Date(from) : new Date(Date.now() - 3600000);
    const toTime = to ? new Date(to) : new Date();

    const diffHours = Math.abs(toTime - fromTime) / 36e5;

    const columns = `
      UNIX_TIMESTAMP(CONVERT_TZ(timestamp_hmi, '+00:00', '-01:00')) * 1000 AS time,
      DATE_FORMAT(timestamp_hmi, '%Y-%m-%dT%H:%i:%sZ') AS fecha_creacion_iso,
      temp_liq AS temp_liq,
      temp_gas AS temp_gas,
      pres_f_gas AS psi_gas,
      pres_f_liq AS psi_liq
    `;

    // Se elimina la clÃƒÂ¡usula LIMIT de la consulta SQL
    const query = `
      SELECT ${columns}
      FROM mediciones
      WHERE timestamp_hmi BETWEEN ? AND ?
        AND pres_f_gas IS NOT NULL
      ORDER BY timestamp_hmi ASC
    `;

    // Se ejecuta la consulta pasando solo los parÃƒÂ¡metros de tiempo
    const [rows] = await pool.execute(query, [fromTime, toTime]);

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        rango_horas: diffHours.toFixed(2),
        total: rows.length,
        desde: fromTime.toISOString(),
        hasta: toTime.toISOString()
      },
      series: rows
    });

  } catch (error) {
    console.error('Error en /api/vp:', error);
    res.status(500).json({
      error: 'Error interno al consultar vp',
      details: error.message
    });
  }
});

// Ã¢Å“â€¦ Endpoint optimizado para Grafana Infinity - Sin lÃƒÂ­mite de filas
app.get(API_PREFIX + '/api/qmr', async (req, res) => {
  try {
    const from = req.query.from;
    const to = req.query.to;

    // Se definen los rangos de tiempo
    const fromTime = from ? new Date(from) : new Date(Date.now() - 3600000);
    const toTime = to ? new Date(to) : new Date();

    const diffHours = Math.abs(toTime - fromTime) / 36e5;

    // Columnas a consultar
    const columns = `
      UNIX_TIMESTAMP(CONVERT_TZ(timestamp_hmi, '+00:00', '-01:00')) * 1000 AS time,
      DATE_FORMAT(timestamp_hmi, '%Y-%m-%dT%H:%i:%sZ') AS fecha_creacion_iso,
      qm_liq,
      qm_gas
    `;

    // Query modificada: Se eliminÃƒÂ³ la clÃƒÂ¡usula LIMIT
    const query = `
      SELECT ${columns}
      FROM mediciones
      WHERE timestamp_hmi BETWEEN ? AND ?
        AND qm_liq IS NOT NULL
      ORDER BY timestamp_hmi ASC
    `;

    // EjecuciÃƒÂ³n de la consulta sin el parÃƒÂ¡metro maxRows
    const [rows] = await pool.execute(query, [fromTime, toTime]);

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        rango_horas: diffHours.toFixed(2),
        total: rows.length,
        desde: fromTime.toISOString(),
        hasta: toTime.toISOString()
      },
      series: rows
    });

  } catch (error) {
    console.error('Error en /api/qmr:', error);
    res.status(500).json({
      error: 'Error interno al consultar qmr',
      details: error.message
    });
  }
});


app.get(API_PREFIX + '/api/pressures', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(CONVERT_TZ(timestamp_hmi, '+00:00', '-01:00')) * 1000 AS time,
        presion_cabezal,
        presion_casing,
        presion_linea,
        presion_macolla
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;

    const [rows] = await pool.execute(query);

    res.json({
      meta: {
        tipo: 'pressures',
        descripcion: 'Presiones actuales del pozo',
        total: rows.length
      },
      data: rows
    });

  } catch (error) {
    console.error('Error en /api/pressures:', error);
    res.status(500).json({
      error: 'Error en pressures',
      details: error.message
    });
  }
});


// Endpoint optimizado para Grafana Infinity - clockmeter
app.get(API_PREFIX + '/api/rholiq', async (req, res) => {
  try {
        const query = `
            SELECT 
            UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time,
            densidad_liq AS densidad,
            pres_f_gas AS psi_gas,
            pres_f_liq AS psi_liq,
            (pres_f_liq - pres_f_gas) AS delta_p
        FROM mediciones
        ORDER BY timestamp_hmi DESC
        LIMIT 1
        `;


    const [rows] = await pool.execute(query);

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        tipo: 'clockmeter',
        total: rows.length,
        descripcion: 'ÃƒÅ¡ltima lectura para relojes de presiÃƒÂ³n y corriente'
      },
      data: rows
    });
  } catch (error) {
    console.error('Error en /api/rholiq:', error);
    res.status(500).json({ error: 'Error interno al consultar rholiq', details: error.message });
  }
});

app.get(API_PREFIX + '/api/densidadapi', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time,
        densidad_liq AS densidad
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;

    const [rows] = await pool.execute(query);

    const RHO_W = 0.999016; // g/cmÃ‚Â³ @60Ã‚Â°F (ASTM/API)
    const data = rows.map(({ time, densidad }) => {
      const rho = Number(densidad);
      if (!Number.isFinite(rho) || rho <= 0) {
        return { time, densidad: densidad ?? null, api: null, error: 'Densidad invÃƒÂ¡lida' };
      }
      const api = 141.5 * (RHO_W / rho) - 131.5;
      return {
        time,
        densidad: Number(rho.toFixed(6)),            // g/cmÃ‚Â³
        api: Number(api.toFixed(3))                  // Ã‚Â°API
      };
    });

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        tipo: 'densidadapi',
        total: data.length,
        descripcion: 'ÃƒÅ¡ltima lectura convertida de densidad (g/cmÃ‚Â³) a grados API @60Ã‚Â°F'
      },
      data
    });
  } catch (error) {
    console.error('Error en /api/densidad-a-api:', error);
    res.status(500).json({ error: 'Error interno al convertir densidad a API', details: error.message });
  }
});

app.get(API_PREFIX + '/api/clockmeter_qm', async (req, res) => {
  try {
    // Tomamos los ÃƒÂºltimos 20 registros para calcular un EMA estable
    // y que la aguja del reloj muestre un valor suavizado en tiempo real
    const alpha = parseFloat(req.query.alpha || '0.05');
    const limit = 20; 

    const query = `
      SELECT 
        UNIX_TIMESTAMP(CONVERT_TZ(timestamp_hmi, '+00:00', '-01:00')) * 1000 AS time,
        qm_liq,
        qm_gas
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT ?
    `;

    const [rows] = await pool.execute(query, [limit]);

    if (rows.length === 0) {
      return res.json({ meta: { total: 0 }, data: [] });
    }

    // Invertimos el array para procesar del mÃƒÂ¡s antiguo al mÃƒÂ¡s reciente (necesario para EMA)
    let series = rows.reverse();
    const keys = ['qm_liq', 'qm_gas'];
    
    // LÃƒÂ³gica de Suavizado EMA
    let lastValidValues = {};
    keys.forEach(key => {
      lastValidValues[key] = Math.max(0, series[0][key] || 0);
    });

    let smoothedResult = {};

    series.forEach((row) => {
      keys.forEach(key => {
        let currentValue = row[key];

        // Filtro de Outliers (Negativos o Nulos)
        if (currentValue < 0 || currentValue === null || currentValue === undefined) {
          currentValue = lastValidValues[key];
        }

        // CÃƒÂ¡lculo EMA
        const smoothedValue = (currentValue * alpha) + (lastValidValues[key] * (1 - alpha));
        lastValidValues[key] = smoothedValue;
        
        // Guardamos el ÃƒÂºltimo valor procesado para enviarlo al reloj
        smoothedResult[key] = Math.round(smoothedValue * 100) / 100;
        smoothedResult.time = row.time;
      });
    });

    // Formateamos la respuesta igual que el endpoint clockmeter original
    res.json({
      meta: {
        tipo: 'clockmeter_qm',
        descripcion: 'Caudales suavizados para relojes (Gauge)',
        alpha: alpha,
        total: 1
      },
      data: [{
        time: smoothedResult.time,
        qm_liq: smoothedResult.qm_liq,
        qm_gas: smoothedResult.qm_gas
      }]
    });

  } catch (error) {
    console.error('Ã¢ÂÅ’ Error en /api/clockmeter_qm:', error);
    res.status(500).json({ 
      error: 'Error interno al consultar clockmeter de caudales', 
      details: error.message 
    });
  }
});

// Endpoint optimizado para Grafana Infinity - gasclockmeter
app.get(API_PREFIX + '/api/clockmeter', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time,
        pres_f_liq AS psi_liq,
        temp_liq AS temp,
        pres_f_gas AS psi_gas
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;

    const [rows] = await pool.execute(query);

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        tipo: 'clockmeter',
        descripcion: 'ÃƒÅ¡ltima lectura para relojes de gas (presiÃƒÂ³n)',
        total: rows.length
      },
      data: rows
    });
  } catch (error) {
    console.error('Error en /api/clockmeter:', error);
    res.status(500).json({ error: 'Error interno al consultar clockmeter', details: error.message });
  }
});

app.get(API_PREFIX + '/api/setpvalve', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time,
        sept_valv AS setpoint_valvula
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;
    const [rows] = await pool.execute(query);

    res.json({
      meta: { tipo: 'setpvalve', descripcion: 'Setpoint de la vÃƒÂ¡lvula', total: rows.length },
      data: rows
    });
  } catch (error) {
    console.error('Error en /api/setpvalve:', error);
    res.status(500).json({ error: 'Error en setpvalve', details: error.message });
  }
});

app.get(API_PREFIX + '/api/possvalve', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time,
        apr_valv AS posicion_valvula
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;
    const [rows] = await pool.execute(query);

    res.json({
      meta: { tipo: 'possvalve', descripcion: 'PosiciÃƒÂ³n actual de la vÃƒÂ¡lvula', total: rows.length },
      data: rows
    });
  } catch (error) {
    console.error('Error en /api/possvalve:', error);
    res.status(500).json({ error: 'Error en possvalve', details: error.message });
  }
});

// Ã‚Â°C Ã¢â€ â€™ Ã‚Â°F
const cToF = c => (c * 9) / 5 + 32;

app.get(API_PREFIX + '/api/temp', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time,
        temp_liq AS temp_liquido,
        temp_gas AS temp_gas
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;
    const [rows] = await pool.execute(query);

    const data = rows.map(({ time, temp_liquido, temp_gas }) => {
      const tl = Number(temp_liquido);
      const tg = Number(temp_gas);

      return {
        time,
        // mismas claves, ahora en Ã‚Â°F
        temp_liquido: Number.isFinite(tl) ? Number(cToF(tl).toFixed(2)) : null,
        temp_gas: Number.isFinite(tg) ? Number(cToF(tg).toFixed(2)) : null
      };
    });

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: { tipo: 'temp', descripcion: 'Temperatura del lÃƒÂ­quido y gas (Ã‚Â°F)', total: data.length },
      data
    });
  } catch (error) {
    console.error('Error en /api/temp:', error);
    res.status(500).json({ error: 'Error en temp', details: error.message });
  }
});

/*
app.get(API_PREFIX + '/api/drivgain', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;
    const [rows] = await pool.execute(query);

    // Si hay resultados, genera valores aleatorios entre 0 y 15%
    const data = rows.map(row => ({
      time: row.time,
      drive_gain_liquido: parseFloat((Math.random() * 15).toFixed(2)),
      drive_gain_gas: parseFloat((Math.random() * 15).toFixed(2))
    }));

    res.json({
      meta: {
        tipo: 'drivgain',
        descripcion: 'Drive Gain lÃƒÂ­quido y gas (simulado entre 0Ã¢â‚¬â€œ15%)',
        total: data.length
      },
      data
    });
  } catch (error) {
    console.error('Error en /api/drivgain:', error);
    res.status(500).json({ error: 'Error en drivgain', details: error.message });
  }
});
*/

// Endpoint para obtener el ÃƒÂºltimo valor de drive gain lÃƒÂ­quido y gas
app.get(API_PREFIX + '/api/drivgain', async (req, res) => {
  try {
    const query = `
      SELECT 
        UNIX_TIMESTAMP(timestamp_hmi) * 1000 AS time,
        driv_gain_liq AS drive_gain_liquido,
        driv_gain_gas AS drive_gain_gas
      FROM mediciones
      ORDER BY timestamp_hmi DESC
      LIMIT 1
    `;
    const [rows] = await pool.execute(query);

    res.json({
      meta: { tipo: 'drivgain', descripcion: 'Drive Gain lÃƒÂ­quido y gas', total: rows.length },
      data: rows
    });
  } catch (error) {
    console.error('Error en /api/drivgain:', error);
    res.status(500).json({ error: 'Error en drivgain', details: error.message });
  }
});



app.get(API_PREFIX + '/api/bsw_lab_changes', async (req, res) => {
  try {
    const maxRows = parseInt(req.query.maxRows || '5000', 10);

    const query = `
      SELECT 
        UNIX_TIMESTAMP(m.timestamp_hmi) * 1000 AS time,
        DATE_FORMAT(CONVERT_TZ(m.timestamp_hmi, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s') AS fecha_local,
        m.bsw_lab
      FROM mediciones m
      JOIN (
        SELECT MIN(id) AS id
        FROM (
          SELECT 
            id,
            bsw_lab,
            timestamp_hmi,
            LAG(bsw_lab) OVER (ORDER BY timestamp_hmi) AS prev_bsw,
            LAG(timestamp_hmi) OVER (ORDER BY timestamp_hmi) AS prev_time
          FROM mediciones
          WHERE bsw_lab IS NOT NULL
        ) AS sub
        WHERE 
          bsw_lab <> prev_bsw
          OR TIMESTAMPDIFF(HOUR, prev_time, timestamp_hmi) >= 1
        GROUP BY timestamp_hmi
        ORDER BY timestamp_hmi DESC
        LIMIT ?
      ) AS filtrado
      ON m.id = filtrado.id
      ORDER BY m.timestamp_hmi DESC;
    `;

    const [rows] = await pool.execute(query, [maxRows]);

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        descripcion: 'HistÃƒÂ³rico de BSW Lab (cambios o lecturas con diferencia >= 1h)',
        total: rows.length,
        timezone: 'America/Caracas (-04:00)',
        orden: 'ASC'
      },
      data: rows,
    });
  } catch (error) {
    console.error('Ã¢ÂÅ’ Error en /api/bsw_lab_changes:', error);
    res.status(500).json({
      error: 'Error interno al consultar bsw_lab_changes',
      details: error.message,
    });
  }
});

app.get(API_PREFIX + '/api/densidad_lab_changes', async (req, res) => {
  try {
    const maxRows = parseInt(req.query.maxRows || '5000', 10);

    const query = `
      SELECT 
        UNIX_TIMESTAMP(m.timestamp_hmi) * 1000 AS time,
        DATE_FORMAT(CONVERT_TZ(m.timestamp_hmi, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s') AS fecha_local,
        m.densidad_lab
      FROM mediciones m
      JOIN (
        SELECT MIN(id) AS id
        FROM (
          SELECT 
            id,
            densidad_lab,
            timestamp_hmi,
            LAG(densidad_lab) OVER (ORDER BY timestamp_hmi) AS prev_densidad,
            LAG(timestamp_hmi) OVER (ORDER BY timestamp_hmi) AS prev_time
          FROM mediciones
          WHERE densidad_lab IS NOT NULL
        ) AS sub
        WHERE 
          densidad_lab <> prev_densidad
          OR TIMESTAMPDIFF(HOUR, prev_time, timestamp_hmi) >= 1
        GROUP BY timestamp_hmi
        ORDER BY timestamp_hmi DESC
        LIMIT ?
      ) AS filtrado
      ON m.id = filtrado.id
      ORDER BY m.timestamp_hmi DESC;
    `;

    const [rows] = await pool.execute(query, [maxRows]);

    res.setHeader('Content-Type', 'application/json');
    res.json({
      meta: {
        descripcion: 'HistÃƒÂ³rico de DENSIDAD Lab (cambios o lecturas con diferencia >= 1h)',
        total: rows.length,
        timezone: 'America/Caracas (-04:00)',
        orden: 'ASC'
      },
      data: rows,
    });
  } catch (error) {
    console.error('Ã¢ÂÅ’ Error en /api/bsw_lab_changes:', error);
    res.status(500).json({
      error: 'Error interno al consultar bsw_lab_changes',
      details: error.message,
    });
  }
});

// Servir archivos estÃƒÂ¡ticos
app.use(express.static(path.join(__dirname, 'public')));

// Servir interfaz web simple
app.get(API_PREFIX, function (req, res) {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Iniciar servidor
server.listen(port, function () {
  console.log('Listening');
  console.log('Servidor API ejecutandose en http://localhost:' + port);
  console.log('Configurado para servidor privado de Namecheap');
  console.log('Socket.IO activo en ' + SOCKET_PATH + ' con transportes: ' + socketTransports.join(', '));
});

module.exports = app;
module.exports.server = server;
module.exports.io = io;
