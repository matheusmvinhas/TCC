# 🔃 Fluxo de Load

#### DAG :recycle:: daily\_infopanel\_to\_api\_disaggregated\_clients.py

#### A dag possui três principais funções:

1. Geração dos índices e partições da tabela nas tabelas last\_price\_by\_client e last\_price\_by\_client\_pointers.
2. Deleção dos dados antigos.
3. Load dos dados novos nas tabelas _last\_price\_by\_client, last\_price\_by\_client\_pointers, last\_price\_by\_client\_counts._

Caracteríticas do Banco de dados:

* PostgreSQL
* Host: prd-last-prices-database-v2.infopriceti.com.br
* Nome: ISA\_LAST\_PRICES\_V2
* Cloud: Oracle
* Tabelas:
  * _last\_price\_by\_client_&#x20;
    * _Particionado por cliente e por data_
  * _last\_price\_by\_client\_pointers_
    * _Particionado por cliente e por data_
  * _last\_price\_by\_client\_counts_
