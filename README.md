# 🧠 Meu Projeto de Integração: HOS Farma & Scanntech

Olá! Sou o Adler, e este é o meu projeto de integração entre o sistema de gestão **HOS Farma** e a plataforma de inteligência **Scanntech**.

O objetivo principal é criar uma ponte robusta e automatizada entre os dois sistemas, garantindo que dados de **vendas**, **cancelamentos**, **fechamentos de caixa** e **promoções** fluam de maneira contínua e segura.

---

## 📦 Como Organizei o Projeto

Para manter tudo limpo e escalável, estruturei o projeto da seguinte forma:

Scanntech/
├── data/                    # Armazena o banco de dados DuckDB com as configurações seguras.
├── gui/                     # Módulo da interface gráfica para o usuário.
│   └── configurador.py
├── integrador/              # O coração da aplicação, onde a mágica acontece.
│   ├── integrador.py        # Orquestra todo o fluxo da integração.
│   ├── autenticacao.py      # Cuida da autenticação HTTP Basic Auth.
│   ├── utils.py             # Funções úteis, como a lógica de requisições com failover.
│   ├── promocoes.py         # Módulo específico para consultar promoções.
│   ├── vendasLote.py        # Responsável por enviar as vendas em lote.
│   └── fechamentosLote.py   # Responsável por enviar os fechamentos diários.
└── logs/                    # Diretório para armazenar os logs de execução (a ser implementado).

---

## ⚙️ O Módulo de Configuração

Para facilitar a vida do usuário, criei uma interface gráfica simples e intuitiva.

### `configurador.py`

Desenvolvido com **Tkinter**, este configurador permite que o usuário insira e salve todas as informações necessárias para a integração:

-   Até 3 URLs base da API (para garantir redundância).
-   Usuário e senha de acesso.
-   Códigos da empresa, filial e PDV.
-   O intervalo (em minutos) para a execução automática do integrador.

Em vez de salvar em um arquivo de texto simples, optei por usar o **DuckDB** para armazenar esses dados. As credenciais sensíveis, como a senha, são criptografadas com `cryptography.fernet` antes de serem salvas, garantindo uma camada extra de segurança.

---

## 🚀 Funcionalidades que Implementei

### 🌐 Consulta de Promoções

Este módulo busca ativamente as promoções cadastradas na Scanntech, permitindo que o HOS Farma tenha sempre informações atualizadas.

**Endpoints que utilizo:**
*   `GET /pmkt-rest-api/v2/minoristas/{idEmpresa}/locales/{idLocal}/promociones`
*   `GET /pmkt-rest-api/minoristas/{idEmpresa}/locales/{idLocal}/promocionesConLimitePorTicket`
*   `GET /pmkt-rest-api/v3/minoristas/{idEmpresa}/locales/{idLocal}/promociones-crm`

### 💳 Envio de Vendas e Cancelamentos

O sistema agrupa as vendas e as envia em lotes de até 350 registros por vez para a API da Scanntech.

**Endpoint principal:**
`POST /api-minoristas/api/v2/minoristas/{idEmpresa}/locales/{idLocal}/cajas/{idCaja}/movimientos/lotes`

#### Detalhe Importante: Canais de Venda

Para que a Scanntech saiba a origem da venda (loja física, e-commerce, Rappi, etc.), eu envio os campos `codigoCanalVenta` e `descripcionCanalVenta`.

**Exemplos de mapeamento:**

| `codigoCanalVenta` | `descripcionCanalVenta` |
|:------------------:|:-----------------------:|
| 1                  | VENTA EN EL LOCAL       |
| 2                  | E-COMMERCE              |
| 3                  | TELEVENTA               |
| 4                  | RAPPI                   |
| 5                  | IFOOD                   |
| 7                  | WHATSAPP                |

Se o sistema de origem for mais simples, eu mapeio `1` para **VENDA EM LOCAL** e `2` para **E-COMMERCE**.

### 📊 Envio de Fechamentos Diários

Ao final do dia, o integrador envia um resumo consolidado das operações de cada caixa (PDV).

**Endpoint utilizado:**
`POST /api-minoristas/api/v2/minoristas/{idEmpresa}/locales/{idLocal}/cajas/{idCaja}/cierresDiarios/lotes`

Isso garante que os totais de vendas líquidas, cancelamentos e a quantidade de transações estejam sempre sincronizados.

---

## ⏱️ Execução Automática

O integrador foi projetado para rodar como um serviço em segundo plano. Ele executa todas as tarefas (envio de vendas, fechamentos e consulta de promoções) em ciclos, conforme o intervalo definido pelo usuário na tela de configuração.

---

## 🛠️ Tecnologias que Utilizei no Projeto

-   **Linguagem:** Python 3
-   **Banco de Dados para Configs:** DuckDB
-   **Comunicação HTTP:** Biblioteca `requests`
-   **Segurança:** `cryptography.fernet` para criptografar as credenciais.
-   **Interface Gráfica:** Tkinter
-   **Formato de Dados:** JSON (UTF-8)
-   **Autenticação:** HTTP Basic Auth

---

## 📌 Notas Finais

-   O `idCaja` corresponde ao código do PDV.
-   O `idLocal` é o código da filial.
-   Toda a comunicação com a API da Scanntech é autenticada e as respostas são tratadas para garantir a integridade dos dados e facilitar o diagnóstico de possíveis problemas.
