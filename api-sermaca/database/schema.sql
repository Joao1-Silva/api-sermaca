-- Esquema de Base de Datos MySQL para HMI Data API
-- Crear la base de datos
CREATE DATABASE IF NOT EXISTS hmi_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Usar la base de datos
USE hmi_data;

-- Crear la tabla para almacenar los datos
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
    sept_valv_1 DECIMAL(10,4) DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Crear índice para búsquedas por fecha
CREATE INDEX idx_mediciones_fecha ON mediciones(fecha_creacion);

-- Comentarios sobre la estructura (MySQL style)
ALTER TABLE mediciones COMMENT = 'Tabla para almacenar datos recibidos del HMI Wecon';
ALTER TABLE mediciones MODIFY COLUMN id INT AUTO_INCREMENT COMMENT 'Identificador único autoincremental';
ALTER TABLE mediciones MODIFY COLUMN fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp de cuando se insertó el registro';
-- Comentarios para nuevos campos
ALTER TABLE mediciones MODIFY COLUMN qm_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal másico';
ALTER TABLE mediciones MODIFY COLUMN q_bruto_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal bruto';
ALTER TABLE mediciones MODIFY COLUMN densidad_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Densidad del fluido';
ALTER TABLE mediciones MODIFY COLUMN bsw_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Basic Sediment and Water';
ALTER TABLE mediciones MODIFY COLUMN q_net_oil_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal neto de petróleo';
ALTER TABLE mediciones MODIFY COLUMN q_agua_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal de agua';
ALTER TABLE mediciones MODIFY COLUMN temp_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Temperatura';
ALTER TABLE mediciones MODIFY COLUMN total_oil_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Total de petróleo';
ALTER TABLE mediciones MODIFY COLUMN pres_f_liq_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Presión de flujo líquido';
ALTER TABLE mediciones MODIFY COLUMN driv_gain_liq_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Drive Gain líquido';
ALTER TABLE mediciones MODIFY COLUMN driv_gain_gas_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Drive Gain gas';
ALTER TABLE mediciones MODIFY COLUMN pres_f_gas_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Presión de flujo gas';
ALTER TABLE mediciones MODIFY COLUMN qv_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal volumétrico';
ALTER TABLE mediciones MODIFY COLUMN temp_cg_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Temperatura del gas';
ALTER TABLE mediciones MODIFY COLUMN d_diluente_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Densidad del diluente';
ALTER TABLE mediciones MODIFY COLUMN apr_valv_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Apertura de válvula';
ALTER TABLE mediciones MODIFY COLUMN sept_valv_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Válvula separadora';
