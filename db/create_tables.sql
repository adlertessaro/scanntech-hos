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

-- Logs de vendas
CREATE TABLE IF NOT EXISTS int_scanntech_vendas_logs (
    venda DOUBLE PRECISION,
    empresa DOUBLE PRECISION,
    estacao DOUBLE PRECISION,
    data_hora_retorno TIMESTAMPTZ,
    tipo_evento VARCHAR(10),
    valor_enviado NUMERIC(15,2),
    id_Lote VARCHAR,
    CONSTRAINT uq_int_scanntech_vendas_logs UNIQUE (venda, empresa, tipo_evento)
);

-- Logs de fechamentos
CREATE TABLE IF NOT EXISTS int_scanntech_fechamentos_logs (
    data_envio DATE,
    empresa DOUBLE PRECISION,
    estacao DOUBLE PRECISION,
    data_retorno DATE,
    id_Lote VARCHAR,
    CONSTRAINT uq_int_scanntech_fechamentos_logs UNIQUE (id_Lote, empresa)
);


-- Tabela CAB das promoção
CREATE TABLE IF NOT EXISTS int_scanntech_promocao (
    data_envio DATE,
    empresa DOUBLE PRECISION,
    nome_promocao VARCHAR (255),
    autor VARCHAR (255),
    tipo VARCHAR (255),
    data_inicio DATE,
    data_fim DATE,
    id VARCHAR (255),
    CONSTRAINT uq_promocao_scanntech UNIQUE (id, empresa)
);

-- Tabela dos produtos da promoção
CREATE TABLE IF NOT EXISTS int_scanntech_promocao_prd (
    id SERIAL PRIMARY KEY,
    id_promocao VARCHAR(255),
    empresa DOUBLE PRECISION,
    codigo_barras VARCHAR(50),
    nome_produto VARCHAR(255),
    quantidade_leva INT,
    quantidade_paga INT,
    preco NUMERIC(10, 2),
    desconto NUMERIC(10, 2) DEFAULT 0,
    tipo_desconto VARCHAR(20),
    criado_em TIMESTAMP DEFAULT NOW(),
    inserido_na_promocao BOOLEAN DEFAULT FALSE
);