Este sistema é um middleware robusto projetado para sincronizar dados de vendas, cancelamentos e fechamentos diários entre o ERP HOS Farma e os servidores da Scanntech. Ele garante a integridade dos dados através de ciclos automáticos de auditoria e manutenção.

🛠️ Tecnologias e Dependências
Para fabricar e rodar este sistema, você precisará do Python 3.9+ e das seguintes bibliotecas:

Núcleo (Core)
psycopg2-binary: Para conexão de alta performance com o banco PostgreSQL.

requests: Para comunicação com a API REST da Scanntech.

pytz: Para manipulação precisa de fusos horários.

cryptography / pycryptodome: Para descriptografar as senhas do banco de dados do arquivo de configuração do ERP.

Interface Gráfica (Configurador)
tkinter: Base da interface.

ttkbootstrap: Para um visual moderno e profissional (baseado em Bootstrap).

Como Instalar:
No terminal, dentro da pasta do projeto, execute:

Bash

pip install psycopg2-binary requests pytz pycryptodome ttkbootstrap
🏗️ Arquitetura dos Componentes
O sistema é dividido em 4 camadas principais:

1. Interface de Controle (configurador.py)
É a torre de comando. Permite configurar URLs, credenciais e, principalmente, forçar ações manuais.

Destaque: Possui uma "Mecânica de Espera" (threading) para que a interface não trave enquanto o sistema processa milhares de vendas no banco de dados.

2. O Coração Processador (loop.py)
Um serviço que roda em segundo plano gerenciando 4 ciclos vitais:


Auditoria (12h): Consulta a Scanntech para saber se eles perderam algum dado e precisam de reenvio.

Promoções (30min): Busca novas ofertas para os PDVs.

Vendas (Intervalo configurável): Envia os cupons fiscais e cancelamentos.

Fechamentos (Diário): Consolida os valores do dia e envia o resumo financeiro.

3. Camada de Inteligência de Dados (payloads/)
Aqui a "mágica" acontece. O sistema transforma dados brutos do SQL em JSONs perfeitos para a Scanntech.

vendas_payload.py: Gerencia a complexidade de descontos, acréscimos e meios de pagamento (PIX, Cartão, Dinheiro).

fechamentos_payload.py: Não confia apenas no que está no ERP; ele calcula o fechamento com base no que a Scanntech realmente recebeu (int_scanntech_vendas_logs), garantindo que o fechamento bata 100% com as vendas enviadas.

4. Módulo de Auditoria e Reset (auditoria_processor.py)
Responsável pelo "Self-Healing" (Auto-cura) do sistema.

Reset de Vendas: Deleta logs antigos e dá um UPDATE na tabela caixa para disparar as triggers e reprocessar as vendas.

Reset de Fechamento: Limpa o id_lote para que o sistema reenvie o dia solicitado pela Scanntech.

🚦 Fluxo de Dados (A "Verdade Única")
O sistema utiliza uma tabela de logs (int_scanntech_vendas_logs) como fonte da verdade.

Uma venda é realizada no ERP.

A Trigger a coloca na fila.

O integrador envia para a Scanntech.

Se a API aceitar, gravamos o id_lote no log.

O Fechamento é gerado somando apenas esses logs de sucesso.

[!TIP] Dica de Engenharia: Se houver divergência de centavos nos últimos 3 dias, o fechamentos_processor.py invalida o fechamento automaticamente e o reenvia corrigido.

📋 Requisitos de Banco de Dados
O sistema espera que o banco de dados possua as seguintes tabelas de integração:

int_scanntech_vendas: Fila de cupons pendentes.

int_scanntech_vendas_logs: Histórico de envios com ID de retorno da API.

int_scanntech_fechamentos: Controle de status dos fechamentos diários.