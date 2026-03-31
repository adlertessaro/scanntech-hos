
-- Tabela de vendas
CREATE TABLE IF NOT EXISTS int_scanntech_vendas (
    venda DOUBLE PRECISION,
    empresa DOUBLE PRECISION,
    estacao DOUBLE PRECISION,
    tentativas INTEGER,
    data_hora_inclusao TIMESTAMPTZ,
    data_hora_tentativa TIMESTAMPTZ,
    erro TEXT,
    CONSTRAINT uq_int_scanntech_vendas UNIQUE (venda, empresa)
);

-- 2. Garante que cada campo existe (adiciona só se faltar)
ALTER TABLE int_scanntech_vendas
    ADD COLUMN IF NOT EXISTS venda DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS empresa DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS estacao DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS tentativas INTEGER,
    ADD COLUMN IF NOT EXISTS data_hora_inclusao TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS data_hora_tentativa TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS erro TEXT;

-- Logs de vendas
CREATE TABLE IF NOT EXISTS int_scanntech_vendas_logs (
    venda DOUBLE PRECISION,
    empresa DOUBLE PRECISION,
    estacao DOUBLE PRECISION,
    data_registro DATE,
    data_hora_retorno TIMESTAMPTZ,
    tipo_evento VARCHAR(10),
    valor_enviado NUMERIC(15,2),
    cupom INTEGER,
    lancamen VARCHAR(2),
    operador VARCHAR(50),
    id_Lote VARCHAR,
    CONSTRAINT uq_int_scanntech_vendas_logs UNIQUE (venda, empresa, tipo_evento)
);

-- 2. Garante que cada campo existe (adiciona só se faltar)
ALTER TABLE int_scanntech_vendas_logs
    ADD COLUMN IF NOT EXISTS venda DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS empresa DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS estacao DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS data_registro DATE,
    ADD COLUMN IF NOT EXISTS data_hora_retorno TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS tipo_evento VARCHAR(10),
    ADD COLUMN IF NOT EXISTS valor_enviado NUMERIC(15,2),
    ADD COLUMN IF NOT EXISTS id_Lote VARCHAR,
    ADD COLUMN IF NOT EXISTS cupom INTEGER,
    ADD COLUMN IF NOT EXISTS lancamen VARCHAR(2),
    ADD COLUMN IF NOT EXISTS operador VARCHAR(50);