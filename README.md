Perfeito, Adler! Aqui está a versão completa e atualizada do seu `README.md` com **todos os detalhes**, incluindo o trecho dos **canais de venda** já incorporado na estrutura certa e formatado para Markdown.

---

```markdown
# 🧠 Integração HOS Farma x Scanntech

Este projeto tem como objetivo realizar a integração entre o sistema **HOS Farma** e a plataforma **Scanntech**, automatizando o envio de **vendas**, **cancelamentos**, **fechamentos diários** e a **consulta de promoções**.

---

## 📦 Estrutura do Projeto

```
Scanntech/
├── config/                  # Armazena o arquivo criptografado settings.config
├── gui/                     # Interface gráfica para configuração (Tkinter)
│   └── configurador.py
├── integrador/              # Núcleo da aplicação
│   ├── integrador.py        # Carrega e descriptografa as configurações
│   ├── autenticacao.py      # Monta autenticação básica (HTTP Basic Auth)
│   ├── utils.py             # Funções genéricas de requisição e failover
│   ├── promocoes.py         # Consulta de promoções
│   ├── vendasLote.py        # Envio de vendas em lote
│   └── fechamentosLote.py   # Envio de fechamentos em lote
└── logs/                    # (Futuramente) Armazenará os logs das integrações
```

---

## ⚙️ Configuração do Sistema

### configurador.py

Interface gráfica (Tkinter) para preencher e salvar configurações de integração de forma criptografada:

- URLs base (até 3)
- Usuário e senha da API
- Código da empresa, filial e PDV
- Intervalo de execução do integrador

As configurações são salvas de forma segura em `settings.config` usando `cryptography.fernet`.

### integrador.py

Responsável por carregar e interpretar o arquivo `settings.config` e disponibilizar os dados para os demais módulos.

### utils.py

Contém a função `fazer_requisicao()` com lógica de:

- Substituição de variáveis (ex: idEmpresa, idLocal)
- Requisições redundantes (failover de URLs)
- Tratamento de erros e exibição dos retornos da API

---

## 📆 Funcionalidades Implementadas

### 🌐 Promoções

Consulta de promoções ativas na base da Scanntech.

**Endpoints:**

- `GET /pmkt-rest-api/v2/minoristas/{idEmpresa}/locales/{idLocal}/promociones`
- `GET /pmkt-rest-api/minoristas/{idEmpresa}/locales/{idLocal}/promocionesConLimitePorTicket`
- `GET /pmkt-rest-api/v3/minoristas/{idEmpresa}/locales/{idLocal}/promociones-crm`

**Estados:** `PENDIENTE`, `ACEPTADA`, `RECHAZADA`  
**Tipos:** `LLEVA_PAGA`, `ADICIONAL_DESCUENTO`, `ADICIONAL_REGALO`, `PRECIO_FIJO`, `DESCUENTO_VARIABLE`, `DESCUENTO_FIJO`

---

### 💳 Vendas

Envio periódico de vendas em lote (até 350 registros por requisição).

**Endpoint:**
```
POST /api-minoristas/api/v2/minoristas/{idEmpresa}/locales/{idLocal}/cajas/{idCaja}/movimientos/lotes
```

#### Campos do JSON de Venda

| Campo | Descrição |
|-------|-----------|
| `fecha` | Data e hora da venda (formato ISO 8601) |
| `numero` | Número de controle (igual ao do cupom impresso). Em cancelamentos, usar prefixo hífen (ex: `-0358`) |
| `descuentoTotal` | Soma dos descontos (itens + subtotal) |
| `recargoTotal` | Soma dos recargos (itens + subtotal) |
| `codigoMoneda` | Código ISO 4217 da moeda (ex: `986` para BRL) |
| `cotizacion` | Cotação do câmbio da moeda usada |
| `total` | Valor total da venda |
| `cancelacion` | `true` para devolução, `false` para venda normal. Enviar os dois registros separadamente |
| `idCliente` | ID no CRM ou programa de fidelidade |
| `documentoCliente` | Documento do cliente (se informado) |
| `codigoCanalVenta` | Código do canal de venda |
| `descripcionCanalVenta` | Descrição do canal de venda |

#### 🛒 Canais de Venda (codigoCanalVenta / descripcionCanalVenta)

Para indicar por qual canal foi realizada a venda, é necessário enviar dois campos:

- `codigoCanalVenta`: valor numérico
- `descripcionCanalVenta`: valor descritivo do canal

##### Exemplos:

| codigoCanalVenta | descripcionCanalVenta |
|------------------|-----------------------|
| 1                | VENTA EN EL LOCAL     |
| 2                | E-COMMERCE            |
| 3                | TELEVENTA             |
| 4                | RAPPI                 |
| 5                | IFOOD                 |
| 6                | APP PROPRIA           |
| 7                | WHATSAPP              |
| 8                | GLOVO                 |

> 💡 Dica: Se o sistema só indica “loja física” ou “e-commerce”, use mapeamentos simples como:
>
> `1 - VENTA EN EL LOCAL`  
> `2 - E-COMMERCE`

---

#### detalhes (array)

| Campo | Descrição |
|-------|-----------|
| `codigoArticulo` | Código interno do produto |
| `codigoBarras` | Código de barras utilizado |
| `descripcionArticulo` | Nome do produto |
| `cantidad` | Quantidade vendida |
| `importeUnitario` | Valor unitário (com impostos, sem descontos/recargos) |
| `importe` | Subtotal: `(importeUnitario * cantidad) - descuento + recargo` |
| `descuento` | Valor de desconto no item |
| `recargo` | Valor de recargo no item |

#### pagos (array)

| Campo | Descrição |
|-------|-----------|
| `codigoTipoPago` | Código da forma de pagamento:<br>9=Dinheiro, 10=Crédito, 11=Cheque, 12=Vale, 13=Débito, 14=QR/PIX |
| `codigoMoneda` | Código ISO 4217 da moeda usada |
| `importe` | Valor pago |
| `cotizacion` | Cotação da moeda do pagamento |
| `codigoProveedorQR` | 1 = PIX, outros = “outros” |
| `codigoBanco` | (opcional) Código do banco utilizado no QR |
| `descripcionBanco` | (opcional) Nome do banco |
| `documentoCliente` | Enviar `null` |
| `bin` | Primeiros 6 ou 8 dígitos do cartão (obrigatório para promoções com cartão) |
| `ultimosDigitosTarjeta` | Últimos 4 dígitos do cartão (idem) |
| `numeroAutorizacion` | Número de autorização da transação |
| `codigoTarjeta` | Enviar `null` |

---

### 📊 Fechamento Diário

Resumo das vendas por caixa (PDV) por dia.

**Endpoint:**
```
POST /api-minoristas/api/v2/minoristas/{idEmpresa}/locales/{idLocal}/cajas/{idCaja}/cierresDiarios/lotes
```

**Campos enviados:** `fechaVentas`, `montoVentaLiquida`, `montoCancelaciones`, `cantidadMovimientos`, `cantidadCancelaciones`  
**Retorno:** `idLote`, `errores`

---

## ⏱ Execução

O integrador executa automaticamente, em ciclos definidos pelo usuário:

- Envia vendas acumuladas
- Envia fechamentos diários
- Consulta promoções ativas

---

## 🛠 Tecnologias Utilizadas

- Python 3
- PostgreSQL
- `requests` (requisições HTTP)
- `cryptography.fernet` (criptografia)
- Tkinter (interface de configuração)
- JSON (UTF-8)
- HTTP Basic Auth

---

## 📌 Observações Finais

- `idCaja` = Código do PDV  
- `idLocal` = Código da filial (geralmente 1 ou 2 em ambientes de homologação)  
- A comunicação segue autenticação básica e validações via API da Scanntech  
- Toda resposta da API deve ser analisada para fins de logging e diagnóstico




Usar duckdb para salvar dados credenciais scanntech