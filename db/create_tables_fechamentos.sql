-- Tabela de controle de fechamentos
CREATE TABLE IF NOT EXISTS int_scanntech_fechamentos (
    data_fechamento DATE,
    empresa DOUBLE PRECISION,
    estacao DOUBLE PRECISION,
    tentativas INTEGER,
    data_hora_inclusao TIMESTAMPTZ,
    data_hora_tentativa TIMESTAMPTZ,
    valor_enviado_vendas NUMERIC(15,2),
    valor_enviado_cancelamentos NUMERIC(15,2),
    id_lote VARCHAR,
    erro TEXT,
    CONSTRAINT uq_int_scanntech_fechamentos UNIQUE (data_fechamento, empresa, estacao)
);

-- 2. Garante que cada campo existe (adiciona só se faltar)
ALTER TABLE int_scanntech_fechamentos
    ADD COLUMN IF NOT EXISTS data_fechamento DATE,
    ADD COLUMN IF NOT EXISTS empresa DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS estacao DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS tentativas INTEGER,
    ADD COLUMN IF NOT EXISTS data_hora_inclusao TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS data_hora_tentativa TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS valor_enviado_vendas NUMERIC(15,2),
    ADD COLUMN IF NOT EXISTS valor_enviado_cancelamentos NUMERIC(15,2),
    ADD COLUMN IF NOT EXISTS id_lote VARCHAR,
    ADD COLUMN IF NOT EXISTS erro TEXT;

-- Logs de fechamentos
CREATE TABLE IF NOT EXISTS int_scanntech_fechamentos_logs (
    data_envio DATE,
    empresa DOUBLE PRECISION,
    estacao DOUBLE PRECISION,
    data_retorno DATE,
    id_Lote VARCHAR,
    CONSTRAINT uq_int_scanntech_fechamentos_logs UNIQUE (id_Lote, empresa)
);

-- 2. Garante que cada campo existe (adiciona só se faltar)
ALTER TABLE int_scanntech_fechamentos_logs
    ADD COLUMN IF NOT EXISTS data_envio DATE,
    ADD COLUMN IF NOT EXISTS empresa DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS estacao DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS data_retorno DATE,
    ADD COLUMN IF NOT EXISTS id_Lote VARCHAR;

