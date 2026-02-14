# 🏭 API de Datos HMI Wecon

**Sistema completo de recolección, almacenamiento y consulta de datos industriales**

Esta API REST está diseñada específicamente para recibir, procesar y almacenar datos provenientes de sistemas HMI (Human Machine Interface) Wecon, proporcionando una solución robusta para el monitoreo industrial en tiempo real.

## 📋 Tabla de Contenidos

1. [Características Principales](#-características-principales)
2. [Arquitectura del Sistema](#-arquitectura-del-sistema)
3. [Estructura del Proyecto](#-estructura-del-proyecto)
4. [Base de Datos MySQL](#-base-de-datos-mysql)
5. [Instalación Local](#-instalación-local)
6. [Despliegue en Servidor Namecheap](#-despliegue-en-servidor-namecheap)
7. [API Endpoints](#-api-endpoints)
8. [Configuración Avanzada](#-configuración-avanzada)
9. [Monitoreo y Logs](#-monitoreo-y-logs)
10. [Solución de Problemas](#-solución-de-problemas)

## 🚀 Características Principales

### **Recolección de Datos**
- ✅ **Recepción automática** de datos desde scripts Lua del HMI Wecon
- ✅ **17 variables industriales** monitoreadas simultáneamente
- ✅ **Validación y sanitización** automática de datos
- ✅ **Timestamp automático** para cada medición

### **Almacenamiento Robusto**
- ✅ **Base de datos MySQL** optimizada para alta concurrencia
- ✅ **Índices optimizados** para consultas rápidas por fecha
- ✅ **Pool de conexiones** para máximo rendimiento
- ✅ **Transacciones seguras** con rollback automático

### **Consulta y Análisis**
- ✅ **Filtros temporales** (últimas X horas)
- ✅ **Paginación** para grandes volúmenes de datos
- ✅ **Exportación a CSV** con nombres descriptivos
- ✅ **Estadísticas en tiempo real** (promedios, totales, rangos)

### **Interfaz y Monitoreo**
- ✅ **Interfaz web integrada** para visualización
- ✅ **Health checks** automáticos de base de datos
- ✅ **Logs detallados** para debugging
- ✅ **CORS configurado** para acceso desde cualquier origen

## 🏗️ Arquitectura del Sistema

```
┌─────────────────┐    HTTP POST     ┌─────────────────┐    MySQL     ┌─────────────────┐
│   HMI Wecon     │ ──────────────► │   API Node.js   │ ──────────► │   Base de Datos │
│   (Script Lua)  │                 │   (Express.js)  │             │     MySQL       │
└─────────────────┘                 └─────────────────┘             └─────────────────┘
                                             │
                                             ▼
                                    ┌─────────────────┐
                                    │  Interfaz Web   │
                                    │   (HTML/CSS)    │
                                    └─────────────────┘
```

### **Flujo de Datos**
1. **HMI Wecon** ejecuta script Lua que recolecta 17 variables industriales
2. **Script Lua** envía datos vía HTTP POST a `/api/datos`
3. **API Node.js** valida, procesa y almacena en MySQL
4. **Usuarios** consultan datos vía endpoints REST o interfaz web
5. **Sistema** genera reportes CSV y estadísticas en tiempo real

## 📁 Estructura del Proyecto

```
api-aguilera/
├── 📄 server.js              # Servidor principal Express.js
├── 📦 package.json           # Dependencias y scripts NPM
├── 🔧 .env.example           # Plantilla variables de entorno
├── 📋 .gitignore             # Archivos excluidos de Git
├── 📖 README.md              # Documentación completa
├── 🗄️ database/
│   ├── schema.sql            # Esquema completo MySQL
│   └── migration.sql         # Script migración (si existe DB)
└── 🌐 public/
    └── index.html            # Interfaz web de consulta
```

## 🗄️ Base de Datos MySQL

### **Tabla Principal: `mediciones`**

La tabla almacena todas las variables industriales del HMI Wecon con timestamps automáticos:

| Campo | Tipo | Descripción | Ejemplo |
|-------|------|-------------|---------|
| `id` | INT AUTO_INCREMENT | Identificador único | 1, 2, 3... |
| `fecha_creacion` | TIMESTAMP | Fecha/hora inserción | 2024-01-15 14:30:25 |
| `qm_1` | DECIMAL(10,4) | Caudal másico | 125.4500 |
| `q_bruto_1` | DECIMAL(10,4) | Caudal bruto | 98.7650 |
| `densidad_1` | DECIMAL(10,4) | Densidad del fluido | 0.8520 |
| `bsw_1` | DECIMAL(10,4) | Basic Sediment & Water | 2.1500 |
| `q_net_oil_1` | DECIMAL(10,4) | Caudal neto petróleo | 96.5000 |
| `q_agua_1` | DECIMAL(10,4) | Caudal de agua | 2.2650 |
| `temp_1` | DECIMAL(10,4) | Temperatura | 45.7800 |
| `total_oil_1` | DECIMAL(10,4) | Total de petróleo | 2456.8900 |
| `pres_f_liq_1` | DECIMAL(10,4) | Presión flujo líquido | 15.2300 |
| `driv_gain_liq_1` | DECIMAL(10,4) | Drive Gain líquido | 1.0250 |
| `driv_gain_gas_1` | DECIMAL(10,4) | Drive Gain gas | 0.9850 |
| `pres_f_gas_1` | DECIMAL(10,4) | Presión flujo gas | 8.4500 |
| `qv_1` | DECIMAL(10,4) | Caudal volumétrico | 110.5600 |
| `temp_cg_1` | DECIMAL(10,4) | Temperatura del gas | 38.9200 |
| `d_diluente_1` | DECIMAL(10,4) | Densidad diluente | 0.7850 |
| `apr_valv_1` | DECIMAL(10,4) | Apertura válvula | 75.5000 |
| `sept_valv_1` | DECIMAL(10,4) | Válvula separadora | 85.2500 |

### **Índices Optimizados**
- **PRIMARY KEY**: `id` (auto-incremental)
- **INDEX**: `idx_mediciones_fecha` en `fecha_creacion` (consultas temporales rápidas)

### **Características Técnicas**
- **Engine**: InnoDB (transacciones ACID, bloqueos a nivel de fila)
- **Charset**: UTF8MB4 (soporte completo Unicode)
- **Collation**: utf8mb4_unicode_ci (ordenamiento internacional)

## 💻 Instalación Local

### **Prerrequisitos**
- Node.js >= 18.0.0
- MySQL >= 8.0
- NPM o Yarn

### **Pasos de Instalación**

1. **Clonar el repositorio**
   ```bash
   git clone <repository-url>
   cd api-aguilera
   ```

2. **Instalar dependencias**
   ```bash
   npm install
   ```

3. **Configurar base de datos MySQL**
   ```bash
   # Crear base de datos
   mysql -u root -p
   CREATE DATABASE hmi_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   exit
   
   # Ejecutar esquema
   mysql -u root -p hmi_data < database/schema.sql
   ```

4. **Configurar variables de entorno**
   ```bash
   cp .env.example .env
   ```
   
   Editar `.env`:
   ```env
   DB_HOST=localhost
   DB_USER=tu_usuario_mysql
   DB_PASSWORD=tu_password_mysql
   DB_NAME=hmi_data
   DB_PORT=3306
   NODE_ENV=development
   PORT=3000
   ```

5. **Ejecutar en desarrollo**
   ```bash
   npm run dev
   ```
   
   La API estará disponible en: `http://localhost:3000`

6. **Verificar instalación**
   ```bash
   # Test de conexión a base de datos
   curl http://localhost:3000/api/test-db
   
   # Test de API
   curl http://localhost:3000/api/status
   ```

## 🚀 Despliegue en Servidor Namecheap

### **Preparación del Servidor**

1. **Acceso SSH al servidor**
   ```bash
   ssh usuario@tu-servidor-namecheap.com
   ```

2. **Instalar Node.js 18+ (si no está instalado)**
   ```bash
   # Ubuntu/Debian
   curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
   sudo apt-get install -y nodejs
   
   # Verificar instalación
   node --version
   npm --version
   ```

3. **Instalar PM2 para gestión de procesos**
   ```bash
   sudo npm install -g pm2
   ```

### **Despliegue de la Aplicación**

1. **Subir código al servidor** (FTP, Git, SCP)
   ```bash
   # Opción 1: Git
   git clone <tu-repositorio>
   
   # Opción 2: SCP desde local
   scp -r ./api-aguilera usuario@servidor:/path/to/app/
   ```

2. **Instalar dependencias**
   ```bash
   cd /path/to/api-aguilera
   npm install --production
   ```

3. **Configurar MySQL en Namecheap**
   - Acceder al **cPanel** de Namecheap
   - Ir a **MySQL Databases**
   - Crear nueva base de datos: `usuario_hmi_data`
   - Crear usuario MySQL con permisos completos
   - Anotar: host, usuario, contraseña, nombre DB

4. **Configurar variables de entorno**
   ```bash
   cp .env.example .env
   nano .env
   ```
   
   Configurar con datos de Namecheap:
   ```env
   DB_HOST=localhost  # o IP del servidor MySQL
   DB_USER=usuario_hmi_user
   DB_PASSWORD=tu_password_seguro
   DB_NAME=usuario_hmi_data
   DB_PORT=3306
   NODE_ENV=production
   PORT=3000
   ```

5. **Crear base de datos**
   ```bash
   mysql -h localhost -u usuario_hmi_user -p usuario_hmi_data < database/schema.sql
   ```

6. **Iniciar aplicación con PM2**
   ```bash
   # Iniciar aplicación
   pm2 start server.js --name "hmi-api"
   
   # Configurar inicio automático
   pm2 save
   pm2 startup
   
   # Verificar estado
   pm2 status
   ```

### **Configuración de Firewall y Proxy**

1. **Abrir puerto en firewall**
   ```bash
   sudo ufw allow 3000
   ```

2. **Configurar Nginx como proxy reverso** (opcional)
   ```nginx
   server {
       listen 80;
       server_name tu-dominio.com;
       
       location / {
           proxy_pass http://localhost:3000;
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection 'upgrade';
           proxy_set_header Host $host;
           proxy_cache_bypass $http_upgrade;
       }
   }
   ```

## 🔌 API Endpoints

### **POST `/api/datos`**
**Descripción**: Recibe y almacena datos del HMI Wecon

**Headers**:
```
Content-Type: application/json
```

**Body** (JSON con 17 variables):
```json
{
    "qm_1": 125.4500,
    "q_bruto_1": 98.7650,
    "densidad_1": 0.8520,
    "bsw_1": 2.1500,
    "q_net_oil_1": 96.5000,
    "q_agua_1": 2.2650,
    "temp_1": 45.7800,
    "total_oil_1": 2456.8900,
    "pres_f_liq_1": 15.2300,
    "driv_gain_liq_1": 1.0250,
    "driv_gain_gas_1": 0.9850,
    "pres_f_gas_1": 8.4500,
    "qv_1": 110.5600,
    "temp_cg_1": 38.9200,
    "d_diluente_1": 0.7850,
    "apr_valv_1": 75.5000,
    "sept_valv_1": 85.2500
}
```

**Respuesta Exitosa** (201):
```json
{
    "message": "Datos almacenados correctamente",
    "data": {
        "id": 1234,
        "fecha_creacion": "2024-01-15T14:30:25.000Z",
        "qm_1": 125.4500,
        "q_bruto_1": 98.7650,
        // ... resto de campos
    }
}
```

**Respuesta Error** (500):
```json
{
    "error": "Error interno del servidor"
}
```

### **GET `/api/datos`**
**Descripción**: Consulta datos con filtros opcionales

**Query Parameters**:
- `horas` (opcional): Filtrar últimas X horas
- `limite` (opcional): Máximo registros a retornar
- `pagina` (opcional): Página para paginación

**Ejemplos**:
```bash
# Últimas 24 horas
GET /api/datos?horas=24

# Últimas 100 mediciones
GET /api/datos?limite=100

# Página 2 con 50 registros por página
GET /api/datos?limite=50&pagina=2

# Últimas 12 horas, máximo 20 registros
GET /api/datos?horas=12&limite=20
```

**Respuesta**:
```json
{
    "total": 150,
    "datos": [
        {
            "id": 1234,
            "fecha_creacion": "2024-01-15T14:30:25.000Z",
            "qm_1": 125.4500,
            "q_bruto_1": 98.7650,
            // ... resto de campos
        }
    ]
}
```

### **GET `/api/datos/csv`**
**Descripción**: Descarga datos en formato CSV

**Query Parameters**:
- `horas`: Filtrar últimas X horas
- `todas=true`: Descargar todos los datos

**Ejemplos**:
```bash
# CSV últimas 48 horas
GET /api/datos/csv?horas=48

# CSV todos los datos
GET /api/datos/csv?todas=true
```

**Respuesta**: Archivo CSV con headers descriptivos

### **GET `/api/estadisticas`**
**Descripción**: Estadísticas básicas de los datos

**Respuesta**:
```json
{
    "total_registros": 5420,
    "fecha_mas_antigua": "2024-01-01T00:00:00.000Z",
    "fecha_mas_reciente": "2024-01-15T14:30:25.000Z",
    "temperatura_promedio": 42.5600,
    "drive_gain_gas_promedio": 0.9750,
    "caudal_bruto_promedio": 95.2300,
    "densidad_promedio": 0.8450,
    "bsw_promedio": 2.3400
}
```

### **GET `/api/test-db`**
**Descripción**: Verifica conexión a base de datos

**Respuesta HTML** (navegador):
```html
✅ Database connected successfully
Connected at: 2024-01-15 14:30:25
```

**Respuesta JSON** (API):
```json
{
    "status": "Database connected successfully",
    "time": "2024-01-15T14:30:25.000Z"
}
```

### **GET `/health`**
**Descripción**: Health check del servidor

**Respuesta**:
```json
{
    "status": "OK",
    "timestamp": "2024-01-15T14:30:25.000Z"
}
```

### **GET `/api/status`**
**Descripción**: Status simple para scripts Lua

**Respuesta**:
```json
{
    "message": "API OK"
}
```

## ⚙️ Configuración Avanzada

### **Variables de Entorno Completas**

```env
# Configuración de Base de Datos
DB_HOST=localhost                    # Host del servidor MySQL
DB_USER=hmi_user                     # Usuario de MySQL
DB_PASSWORD=password_seguro          # Contraseña de MySQL
DB_NAME=hmi_data                     # Nombre de la base de datos
DB_PORT=3306                         # Puerto de MySQL

# Configuración del Servidor
PORT=3000                            # Puerto de la aplicación
NODE_ENV=production                  # Entorno (development/production)

# Configuración de Pool de Conexiones MySQL
DB_CONNECTION_LIMIT=10               # Máximo conexiones simultáneas
DB_QUEUE_LIMIT=0                     # Límite de cola (0 = sin límite)
DB_WAIT_FOR_CONNECTIONS=true         # Esperar conexiones disponibles
```

### **Optimización de Rendimiento**

1. **Pool de Conexiones MySQL**
   ```javascript
   // Configuración optimizada en server.js
   const pool = mysql.createPool({
       connectionLimit: 10,      // Máx. 10 conexiones simultáneas
       queueLimit: 0,           // Sin límite de cola
       acquireTimeout: 60000,   // Timeout 60s para obtener conexión
       timeout: 60000,          // Timeout 60s para queries
       reconnect: true,         // Reconectar automáticamente
       idleTimeout: 300000      // Cerrar conexiones inactivas (5min)
   });
   ```

2. **Índices de Base de Datos**
   ```sql
   -- Índice principal por fecha (ya incluido)
   CREATE INDEX idx_mediciones_fecha ON mediciones(fecha_creacion);
   
   -- Índices adicionales para consultas específicas
   CREATE INDEX idx_temp_fecha ON mediciones(temp_1, fecha_creacion);
   CREATE INDEX idx_caudal_fecha ON mediciones(q_bruto_1, fecha_creacion);
   ```

3. **Configuración PM2 Avanzada**
   ```bash
   # Archivo ecosystem.config.js
   module.exports = {
     apps: [{
       name: 'hmi-api',
       script: 'server.js',
       instances: 'max',        # Usar todos los CPU cores
       exec_mode: 'cluster',    # Modo cluster para balanceo
       max_memory_restart: '1G', # Reiniciar si usa >1GB RAM
       error_file: './logs/err.log',
       out_file: './logs/out.log',
       log_file: './logs/combined.log',
       time: true
     }]
   };
   
   # Iniciar con configuración
   pm2 start ecosystem.config.js
   ```

## 📊 Monitoreo y Logs

### **Comandos PM2 Útiles**

```bash
# Ver estado de aplicaciones
pm2 status

# Ver logs en tiempo real
pm2 logs hmi-api

# Ver logs específicos
pm2 logs hmi-api --lines 100

# Monitoreo en tiempo real
pm2 monit

# Reiniciar aplicación
pm2 restart hmi-api

# Recargar sin downtime
pm2 reload hmi-api

# Detener aplicación
pm2 stop hmi-api

# Eliminar de PM2
pm2 delete hmi-api

# Guardar configuración actual
pm2 save

# Ver información detallada
pm2 describe hmi-api
```

### **Logs de la Aplicación**

La aplicación genera logs automáticos para:

1. **Conexiones de base de datos**
   ```
   [2024-01-15 14:30:25] Database connected successfully
   [2024-01-15 14:30:26] Server listening on port 3000
   ```

2. **Inserción de datos**
   ```
   [2024-01-15 14:31:00] Datos insertados: {id: 1234, qm_1: 125.45, ...}
   ```

3. **Errores de base de datos**
   ```
   [2024-01-15 14:31:15] Error al insertar datos: Connection timeout
   ```

### **Monitoreo de Base de Datos**

```sql
-- Ver estadísticas de la tabla
SELECT 
    COUNT(*) as total_registros,
    MIN(fecha_creacion) as primer_registro,
    MAX(fecha_creacion) as ultimo_registro,
    AVG(temp_1) as temp_promedio
FROM mediciones;

-- Ver registros por día
SELECT 
    DATE(fecha_creacion) as fecha,
    COUNT(*) as registros_dia
FROM mediciones 
GROUP BY DATE(fecha_creacion)
ORDER BY fecha DESC;

-- Ver tamaño de la tabla
SELECT 
    table_name AS 'Tabla',
    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Tamaño (MB)'
FROM information_schema.TABLES 
WHERE table_schema = 'hmi_data' 
AND table_name = 'mediciones';
```

## 🔧 Solución de Problemas

### **Problemas Comunes**

#### **1. Error de Conexión a MySQL**
```
Error: connect ECONNREFUSED 127.0.0.1:3306
```

**Soluciones**:
```bash
# Verificar que MySQL esté ejecutándose
sudo systemctl status mysql

# Iniciar MySQL si está detenido
sudo systemctl start mysql

# Verificar puerto MySQL
netstat -tlnp | grep :3306

# Verificar credenciales en .env
cat .env | grep DB_
```

#### **2. Error "Table doesn't exist"**
```
Error: Table 'hmi_data.mediciones' doesn't exist
```

**Soluciones**:
```bash
# Verificar que la base de datos existe
mysql -u root -p -e "SHOW DATABASES;"

# Crear base de datos si no existe
mysql -u root -p -e "CREATE DATABASE hmi_data;"

# Ejecutar esquema
mysql -u root -p hmi_data < database/schema.sql

# Verificar tabla creada
mysql -u root -p hmi_data -e "DESCRIBE mediciones;"
```

#### **3. Error de Permisos MySQL**
```
Error: Access denied for user 'hmi_user'@'localhost'
```

**Soluciones**:
```sql
-- Crear usuario con permisos
CREATE USER 'hmi_user'@'localhost' IDENTIFIED BY 'password_seguro';
GRANT ALL PRIVILEGES ON hmi_data.* TO 'hmi_user'@'localhost';
FLUSH PRIVILEGES;

-- Verificar permisos
SHOW GRANTS FOR 'hmi_user'@'localhost';
```

#### **4. Puerto 3000 ya en uso**
```
Error: listen EADDRINUSE :::3000
```

**Soluciones**:
```bash
# Ver qué proceso usa el puerto
sudo lsof -i :3000

# Matar proceso específico
sudo kill -9 <PID>

# Cambiar puerto en .env
echo "PORT=3001" >> .env
```

#### **5. Memoria insuficiente**
```
Error: JavaScript heap out of memory
```

**Soluciones**:
```bash
# Aumentar memoria para Node.js
node --max-old-space-size=4096 server.js

# Configurar en PM2
pm2 start server.js --node-args="--max-old-space-size=4096"

# Optimizar consultas grandes
# Usar paginación en lugar de consultar todos los datos
```

### **Verificación de Salud del Sistema**

```bash
# Script de verificación completa
#!/bin/bash

echo "=== Verificación API HMI ==="

# 1. Verificar Node.js
echo "Node.js version: $(node --version)"

# 2. Verificar PM2
echo "PM2 status:"
pm2 status

# 3. Verificar MySQL
echo "MySQL status:"
sudo systemctl status mysql --no-pager

# 4. Verificar API
echo "API Health Check:"
curl -s http://localhost:3000/health | jq .

# 5. Verificar Base de Datos
echo "Database Test:"
curl -s http://localhost:3000/api/test-db | jq .

# 6. Verificar últimos logs
echo "Últimos logs:"
pm2 logs hmi-api --lines 5 --nostream

echo "=== Verificación Completa ==="
```

### **Backup y Restauración**

```bash
# Backup de base de datos
mysqldump -u hmi_user -p hmi_data > backup_$(date +%Y%m%d_%H%M%S).sql

# Restaurar backup
mysql -u hmi_user -p hmi_data < backup_20240115_143025.sql

# Backup automático diario (crontab)
0 2 * * * /usr/bin/mysqldump -u hmi_user -p'password' hmi_data > /backups/hmi_$(date +\%Y\%m\%d).sql
```

## 🛠️ Tecnologías Utilizadas

| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| **Node.js** | ≥18.0.0 | Runtime de JavaScript |
| **Express.js** | ^4.18.2 | Framework web REST API |
| **MySQL** | ≥8.0 | Base de datos relacional |
| **mysql2** | ^3.6.0 | Driver MySQL con soporte async/await |
| **csv-stringify** | ^6.3.0 | Generación de archivos CSV |
| **cors** | ^2.8.5 | Manejo de políticas CORS |
| **PM2** | Latest | Gestor de procesos Node.js |

---

## 📞 Soporte Técnico

Para soporte técnico o consultas sobre la implementación:

1. **Revisar logs**: `pm2 logs hmi-api`
2. **Verificar base de datos**: `GET /api/test-db`
3. **Consultar documentación**: Este README
4. **Verificar configuración**: Variables de entorno en `.env`

**Desarrollado para sistemas industriales HMI Wecon con MySQL en servidor Namecheap**

### Despliegue de la Aplicación

1. **Subir el código al servidor** (via FTP, Git, etc.)

2. **Instalar dependencias:**
   ```bash
   cd /path/to/your/api-aguilera
   npm install
   ```

3. **Configurar variables de entorno:**
   ```bash
   cp .env.example .env
   nano .env
   ```
   Configurar con las credenciales de MySQL de Namecheap:
   - `DB_HOST`: Host de MySQL de Namecheap
   - `DB_USER`: Usuario de MySQL
   - `DB_PASSWORD`: Contraseña de MySQL
   - `DB_NAME`: Nombre de la base de datos
   - `DB_PORT`: 3306 (puerto estándar de MySQL)
   - `NODE_ENV`: production
   - `PORT`: 3000 (o el puerto que prefieras)

4. **Iniciar la aplicación con PM2:**
   ```bash
   pm2 start server.js --name "hmi-api"
   pm2 save
   pm2 startup
   ```

## Base de Datos MySQL

### Configuración de la Base de Datos

El esquema completo de MySQL se encuentra en `database/schema.sql`. Para configurar la base de datos:

1. **Acceder a MySQL en Namecheap:**
   - Usar phpMyAdmin desde el panel de control de Namecheap
   - O conectar via cliente MySQL:
   ```bash
   mysql -h tu_host_mysql -u tu_usuario -p
   ```

2. **Ejecutar el esquema:**
   - Copiar y ejecutar el contenido de `database/schema.sql` en phpMyAdmin
   - O ejecutar desde línea de comandos:
   ```bash
   mysql -h tu_host_mysql -u tu_usuario -p < database/schema.sql
   ```

### Estructura de la tabla `mediciones`

```sql
CREATE TABLE mediciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    qm_1 DECIMAL(10,4) DEFAULT 0,
    q_bruto_1 DECIMAL(10,4) DEFAULT 0,
    densidad_1 DECIMAL(10,4) DEFAULT 0,
    bsw_1 DECIMAL(10,4) DEFAULT 0,
    q_net_oil_1 DECIMAL(10,4) DEFAULT 0,
    q_agua_1 DECIMAL(10,4) DEFAULT 0,
    temp_1 DECIMAL(10,4) DEFAULT 0,
    total_oil_1 DECIMAL(10,4) DEFAULT 0,
    pres_f_liq_1 DECIMAL(10,4) DEFAULT 0,
    driv_gain_liq_1 DECIMAL(10,4) DEFAULT 0,
    driv_gain_gas_1 DECIMAL(10,4) DEFAULT 0,
    pres_f_gas_1 DECIMAL(10,4) DEFAULT 0,
    qv_1 DECIMAL(10,4) DEFAULT 0,
    temp_cg_1 DECIMAL(10,4) DEFAULT 0,
    d_diluente_1 DECIMAL(10,4) DEFAULT 0,
    apr_valv_1 DECIMAL(10,4) DEFAULT 0,
    sept_valv_1 DECIMAL(10,4) DEFAULT 0,

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Índice para optimizar consultas por fecha
CREATE INDEX idx_mediciones_fecha ON mediciones(fecha_creacion);
```

### Migración de Base de Datos Existente

Si ya tienes una tabla `mediciones` creada, ejecuta el script `database/migration.sql` para agregar los nuevos campos:

```bash
mysql -h tu_host_mysql -u tu_usuario -p < database/migration.sql
```

### Configuración de MySQL en Namecheap:

1. **Crear base de datos** desde el panel de control de Namecheap
2. **Crear usuario MySQL** con permisos completos sobre la base de datos
3. **Anotar las credenciales** para configurar en el archivo `.env`

## API Endpoints

### POST `/api/datos`
Recibe datos del script Lua y los almacena en la base de datos.

**Body (Nuevos campos):**
```json
{
    "qm_1": 123.45,
    "q_bruto_1": 67.89,
    "densidad_1": 12.34,
    "bsw_1": 56.78,
    "q_net_oil_1": 90.12,
    "q_agua_1": 34.56,
    "temp_1": 78.90,
    "total_oil_1": 23.45,
    "pres_f_liq_1": 45.67,
    "driv_gain_liq_1": 89.01,
    "driv_gain_gas_1": 12.34,
    "pres_f_gas_1": 56.78,
    "qv_1": 90.12,
    "temp_cg_1": 34.56,
    "d_diluente_1": 78.90,
    "apr_valv_1": 23.45,
    "sept_valv_1": 67.89
}
```

**Body (Campos legacy - compatibilidad):**
```json
{
    "oa_brute": 123.45,
    "boensidad": 67.89,
    "ssw": 12.34,
    "a_neto": 56.78,
    "temp_gas": 90.12,
    "densidad_diluente": 34.56,
    "drive_gain_gas": 78.90,
    "column1": 23.45
}
```

### GET `/api/datos`
Consulta datos con filtros opcionales.

**Query Parameters:**
- `horas`: Filtrar por últimas X horas
- `limite`: Número máximo de registros
- `pagina`: Página para paginación

### GET `/api/datos/csv`
Descarga datos en formato CSV.

**Query Parameters:**
- `horas`: Filtrar por últimas X horas
- `todas=true`: Descargar todos los datos

### GET `/api/estadisticas`
Obtiene estadísticas básicas de los datos.

## Interfaz Web

Accede a `/` para usar la interfaz web que permite:
- Consultar datos por rango de horas
- Descargar datos en CSV
- Ver estadísticas básicas

## Scripts Disponibles

- `npm start`: Ejecutar en producción
- `npm run dev`: Ejecutar en desarrollo con nodemon
- `npm run build`: Comando de build (no necesario para este proyecto)

## Tecnologías Utilizadas

- **Node.js** - Runtime de JavaScript
- **Express.js** - Framework web
- **MySQL** - Base de datos
- **mysql2** - Driver de MySQL para Node.js
- **CSV-Stringify** - Generación de archivos CSV
- **CORS** - Manejo de políticas de origen cruzado

## Configuración de Producción

El proyecto está optimizado para servidor privado de Namecheap con:
- Variables de entorno para configuración de MySQL
- Pool de conexiones MySQL optimizado
- Configuración de puerto dinámico
- Archivos estáticos servidos desde `/public`
- Gestión de procesos con PM2

## Soporte

Para problemas o preguntas sobre el despliegue, revisa:
1. Los logs de PM2: `pm2 logs hmi-api`
2. Las variables de entorno están correctamente configuradas en `.env`
3. La base de datos MySQL está accesible desde el servidor
4. La tabla `mediciones` existe en la base de datos
5. El firewall del servidor permite conexiones en el puerto configurado

## Comandos Útiles de PM2

```bash
# Ver estado de la aplicación
pm2 status

# Ver logs en tiempo real
pm2 logs hmi-api

# Reiniciar la aplicación
pm2 restart hmi-api

# Detener la aplicación
pm2 stop hmi-api

# Eliminar la aplicación de PM2
pm2 delete hmi-api
```
