-- Remove a trigger e a função anterior, se existirem
DROP TRIGGER IF EXISTS int_scanntech_vendas ON caixa;
DROP FUNCTION IF EXISTS trg_func_int_scanntech_vendas();

-- Cria a função da trigger com a lógica correta usando "estacao"
CREATE OR REPLACE FUNCTION trg_func_int_scanntech_vendas()
RETURNS trigger AS $$
DECLARE
    ja_enviado BOOLEAN;
    v_tipo_evento VARCHAR(10);
BEGIN
    IF (SESSION_USER = 'INTWEBSYNC' OR SESSION_USER = 'INTEGRADORWEB') THEN
        RETURN NULL;
    END IF;

    -- Apenas para tipos de lançamento válidos e com estação diferente de 0
    IF (NEW.lancamen IN ('VV', 'VC', 'VP', 'CR', 'CH', 'DV', 'CC')
        AND NEW.estacao <> 0) THEN
    BEGIN
        -- Determina o tipo de evento (VENDA, CC, ou DV)
        IF (NEW.lancamen IN ('CC', 'DV')) THEN
            v_tipo_evento := NEW.lancamen;
        ELSE
            v_tipo_evento := 'VENDA';
        END IF;

        -- Verifica se ESTE EVENTO específico já foi enviado
        SELECT EXISTS (
            SELECT 1 FROM int_scanntech_vendas_logs
            WHERE venda = NEW.venda 
              AND empresa = NEW.empresa 
              AND tipo_evento = v_tipo_evento 
              AND id_Lote IS NOT NULL
        ) INTO ja_enviado;

        -- Se este evento ainda não foi enviado, insere na fila
        IF NOT ja_enviado THEN
            INSERT INTO int_scanntech_vendas (venda, empresa, estacao, tentativas, data_hora_inclusao)
            VALUES (NEW.venda, NEW.empresa, NEW.estacao, 0, CURRENT_TIMESTAMP)
            ON CONFLICT (venda, empresa) DO UPDATE
            SET estacao = EXCLUDED.estacao,
                tentativas = 0,
                data_hora_inclusao = CURRENT_TIMESTAMP;
        END IF;
    END;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Cria novamente a trigger
CREATE TRIGGER int_scanntech_vendas
AFTER INSERT OR UPDATE ON caixa
FOR EACH ROW
EXECUTE FUNCTION trg_func_int_scanntech_vendas();