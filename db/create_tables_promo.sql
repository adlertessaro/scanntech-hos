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

-- 2. Garante que cada campo existe (adiciona só se faltar)
ALTER TABLE int_scanntech_promocao
    ADD COLUMN IF NOT EXISTS data_envio DATE,
    ADD COLUMN IF NOT EXISTS empresa DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS nome_promocao VARCHAR (255),
    ADD COLUMN IF NOT EXISTS autor VARCHAR (255),
    ADD COLUMN IF NOT EXISTS tipo VARCHAR (255),
    ADD COLUMN IF NOT EXISTS data_inicio DATE,
    ADD COLUMN IF NOT EXISTS data_fim DATE,
    ADD COLUMN IF NOT EXISTS id VARCHAR (255);

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

-- 2. Garante que cada campo existe (adiciona só se faltar)
ALTER TABLE int_scanntech_promocao_prd
    ADD COLUMN IF NOT EXISTS id_promocao VARCHAR(255),
    ADD COLUMN IF NOT EXISTS empresa DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS codigo_barras VARCHAR(50),
    ADD COLUMN IF NOT EXISTS nome_produto VARCHAR(255),
    ADD COLUMN IF NOT EXISTS quantidade_leva INT,
    ADD COLUMN IF NOT EXISTS quantidade_paga INT,
    ADD COLUMN IF NOT EXISTS preco NUMERIC(10, 2),
    ADD COLUMN IF NOT EXISTS desconto NUMERIC(10, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tipo_desconto VARCHAR(20),
    ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS inserido_na_promocao BOOLEAN DEFAULT FALSE;

-- 1. Garante que a coluna existe na promocao_cab
ALTER TABLE promocao_cab ADD COLUMN IF NOT EXISTS id_scanntech VARCHAR(255);

-- 2. Cria o índice único na promocao_cab (Mais seguro que CONSTRAINT se houver lixo)
CREATE UNIQUE INDEX IF NOT EXISTS uk_promocao_cab_id_scanntech 
ON promocao_cab (id_scanntech) 
WHERE id_scanntech IS NOT NULL;

-- 3. Adiciona as chaves de integração nos itens e lojas
ALTER TABLE promocao_prd ADD COLUMN IF NOT EXISTS scanntech_item_key VARCHAR(255);
ALTER TABLE promocao_prd_lojas ADD COLUMN IF NOT EXISTS scanntech_loja_key VARCHAR(255);

-- 4. Cria os índices de unicidade (O "Juiz" para o ON CONFLICT)
CREATE UNIQUE INDEX IF NOT EXISTS uk_scanntech_item 
ON promocao_prd (scanntech_item_key) 
WHERE scanntech_item_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uk_scanntech_loja 
ON promocao_prd_lojas (scanntech_loja_key) 
WHERE scanntech_loja_key IS NOT NULL;