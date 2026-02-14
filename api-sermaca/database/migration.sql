-- Script de migración para agregar nuevos campos a tabla existente
-- Ejecutar SOLO si ya tienes una tabla 'mediciones' creada anteriormente

USE hmi_data;

-- Agregar nuevos campos del script Lua
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS qm_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal másico';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS q_bruto_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal bruto';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS densidad_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Densidad del fluido';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS bsw_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Basic Sediment and Water';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS q_net_oil_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal neto de petróleo';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS q_agua_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal de agua';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS temp_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Temperatura';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS total_oil_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Total de petróleo';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS pres_f_liq_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Presión de flujo líquido';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS driv_gain_liq_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Drive Gain líquido';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS driv_gain_gas_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Drive Gain gas';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS pres_f_gas_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Presión de flujo gas';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS qv_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Caudal volumétrico';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS temp_cg_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Temperatura del gas';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS d_diluente_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Densidad del diluente';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS apr_valv_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Apertura de válvula';
ALTER TABLE mediciones ADD COLUMN IF NOT EXISTS sept_valv_1 DECIMAL(10,4) DEFAULT 0 COMMENT 'Válvula separadora';

-- Verificar que los campos se agregaron correctamente
DESCRIBE mediciones;
