# 🎲 Fluxo de geração dos dados

#### DAG :recycle:: daily\_ftp.py

1. Dependencia de run\_etl\_panel\_ftp (Geração da base do FTP) da DAG daily\_final.py (DAG do InfoPanel)
2.  run\_etl\_ftp\_clients\_task:

    _Task responsável para fazer o cruzamento dos dados de contrato do cliente e gerar base de possibilidades para cada cliente._
3.  run\_etl\_export\_database\_consinco\_current\_clients\_task:

    Task responsável por rodar infopanel\_exporter\_api\_disaggregated.py e gerar os dados last\_prices para cada cliente.

### infopanel\_exporter\_api\_disaggregated.py

1. Filtro dos clientes que possuem integração do api desagregada:

```python
infopanel_exported = spark.read.orc(
    "s3a://ip-byte-pool/infoprice/curated/ftp/ftp_clients/"
)

# Filtering FTP Clients
layout_client_to_process = (
    infopanel_exported.filter(
        infopanel_exported.cliente_novo == process_new_exporters
    )
    .filter(infopanel_exported.layout == "api_disaggregated")
    .cache()
)
```

2. Leitura, deduplicação e normalização dos filtros desejados pelo cliente. Estes filtros se encontram no banco do InfoPanel com o host: `prd-infopanel-database.infopriceti.com.br`, na tabela `last_prices.last_prices_filter.`

```python
last_prices_filter = spark.sql(
    """
    WITH dedup AS (
        SELECT
            p.*,
            ROW_NUMBER() OVER (PARTITION BY p.cd_cliente, p.filter_key ORDER BY p.run DESC) AS row_id
        FROM infopanel.last_prices_filter AS p
        WHERE cd_cliente is not null
    )
    SELECT
        *
    FROM dedup
    WHERE
        row_id = 1
"""
)
last_prices_filter.drop("row_id").cache().createOrReplaceTempView(
    "last_prices_filter"
)

# Transform filter_value into a list
df_transformed = last_prices_filter.withColumn(
    "filter_value_list", split(last_prices_filter["filter_value"], "\|")
)

# Explode filter_value_list
filters = df_transformed.withColumn(
    "exploded_filter_value", explode("filter_value_list")
).select("cd_cliente", "filter_key", "exploded_filter_value", "run")

client_to_filter = (
    df_transformed.select("cd_cliente")
    .distinct()
    .agg(collect_list("cd_cliente"))
    .collect()[0][0]
)

normalized_filters = filters.withColumn(
    "exploded_filter_value",
    F.when(
        (col("filter_key") == "gtin") | (col("filter_key") == "cnpj"),
        F.lpad(col("exploded_filter_value"), 14, "0"),
    )
    .when((col("filter_key") == "uf"), F.upper(col("exploded_filter_value")))
    .otherwise(col("exploded_filter_value")),
).cache()
```

3. Geração dinamicamente de uma query, onde o filtro de cada cliente é aplicado. O processo foi criado deste modo pensando no paralelismo da execução da query e na possibilidade de adição e remoção de filtros por parte de cada cliente separado.

```python
client_list = []
for clients_key in client_to_filter:
    filter_from_client = (
        normalized_filters.filter(normalized_filters.cd_cliente == clients_key)
        .select("filter_key")
        .distinct()
        .agg(collect_list("filter_key"))
        .collect()[0][0]
    )
    staging_filter = (clients_key, filter_from_client)
    client_list.append(staging_filter)
    if len(filter_from_client) > 0:
        logger.info(f"client {clients_key} has {len(filter_from_client)} filters.")
    else:
        logger.info(f"client {clients_key} has no filters.")

final_query = ""
for client in client_list:
    text_filter = f""" query_{client[0]} AS (
            SELECT
                *
            FROM etl_layout
            WHERE 1=1
                AND cd_cliente = '{client[0]}'"""
    for filter_key in client[1]:
        filter = (
            normalized_filters.filter(normalized_filters.cd_cliente == client[0])
            .filter(normalized_filters.filter_key == filter_key)
            .distinct()
            .agg(collect_list("exploded_filter_value"))
            .collect()[0][0]
        )
        string_list = [str(element) for element in filter]
        delimiter = "', '"
        result_string = delimiter.join(string_list)
        text_filter += f" AND {filter_key} in ('{result_string}')"
    final_query += text_filter + " ),"

final_query = final_query[:-1]

for client in client_list:
    final_query += f" SELECT * FROM query_{client[0]} UNION ALL "

final_query = final_query[:-10]
final_query = "WITH " + final_query
```

4. Aplicação das restrições de contrato por parte de cada cliente:

```python
etl_layout = (
    spark.sql(
        f"""
    WITH base_store_type_and_region_city AS(
        SELECT DISTINCT
        cd_cliente,
        regiao,
        canal,
        cidade,
        in_filtro_secao_categoria,
        contrato_shopping_brasil
    FROM layout_client_to_process
    ),
    tudo AS(
        SELECT
            {colunas}
        FROM base_store_type_and_region_city AS exporter
        INNER JOIN last_prices AS p ON p.uf = exporter.regiao AND p.tipo_loja = exporter.canal AND exporter.cidade = p.cidade
            AND exporter.in_filtro_secao_categoria = 'TODOS'
        WHERE p.preco_pago > 0.0
        AND   p.anonimo = 'false'

        UNION ALL

        SELECT
            {colunas}
        FROM layout_client_to_process AS exporter
        INNER JOIN last_prices AS p ON p.uf = exporter.regiao AND p.tipo_loja = exporter.canal AND exporter.cidade = p.cidade AND exporter.secao = p.secao
            AND exporter.categoria = p.categoria AND exporter.in_filtro_secao_categoria <> 'TODOS'
        WHERE p.preco_pago > 0.0
        AND   p.anonimo = 'false'
    ),
    dedup AS (
        SELECT
        t.*,
        ROW_NUMBER() OVER (PARTITION BY t.gtin, t.cnpj, t.cd_cliente ORDER BY t.run DESC, t.data_preco DESC) AS row_id
        FROM tudo t
    )
    SELECT
        *
    FROM dedup
    WHERE
        row_id = 1
"""
        # Registering etl_layout before normalized_filters
    )
    .withColumn("marca_propria", F.col("marca_propria").cast("boolean"))
    .withColumn("flag_promocao", F.col("flag_promocao").cast("boolean"))
    .withColumn("cnpj_numeric", F.col("cnpj").cast("long"))
    .withColumn("run", F.to_date(F.col("run"), "yyyy-MM-dd"))
    .cache()
)

etl_layout.createOrReplaceTempView("etl_layout")
```

5. Aplicação dos filtros desejados por cada cliente:

```python
client_with_filter = spark.sql(final_query)
client_without_filter = etl_layout.filter(
    ~etl_layout.cd_cliente.isin(client_to_filter)
)

etl_layout = (
    client_with_filter.union(client_without_filter)
    .cache()
    .createOrReplaceTempView("etl_layout_after_filters")
)
```

6. Geração da tabela last\_price\_by\_client:

```python
# Base by Clients
spark.sql("SELECT * FROM etl_layout_after_filters").drop(
    "canal", "regiao_uf", "row_id"
).coalesce(50).write.mode("append").partitionBy(
    "partition_cd_cliente", "partition_run"
).format(
    "orc"
).save(
    FINAL_PATH_ORC
)

```

7. Geração da tabela last\_prices\_by\_client\_counts:

```python
spark.sql(
    """
    SELECT
        t.cd_cliente,
        t.run as data_run,
        count(1) AS total_results
    FROM etl_layout_after_filters AS t
    GROUP BY
        t.cd_cliente,
        t.run
"""
).withColumn("partition_cd_cliente", F.col("cd_cliente")).withColumn(
    "partition_run", F.lit(EXECUTION_DATE)
).write.mode(
    "append"
).partitionBy(
    "partition_cd_cliente", "partition_run"
).format(
    "orc"
).save(
    FINAL_COUNTS_PATH_ORC
)
```

8. Geração da tabela last\_prices\_by\_client\_pointers:

```python
spark.sql(
    f"""
    WITH ordered_base AS (
        SELECT
            t.cd_cliente,
            t.run,
            t.cnpj,
            t.gtin,
            ROW_NUMBER() OVER (PARTITION BY t.cd_cliente, t.run ORDER BY t.cnpj, t.gtin) AS ROW_ID
        FROM etl_layout_after_filters AS t
    )
    SELECT
        o.cd_cliente,
        o.run as data_run,
        o.cnpj,
        o.gtin,
        CEIL(o.ROW_ID / {PAGE_SIZE}) AS page_number
    FROM ordered_base AS o
    WHERE o.ROW_ID % {PAGE_SIZE} = 0
"""
).withColumn("partition_cd_cliente", F.col("cd_cliente")).withColumn(
    "partition_run", F.lit(EXECUTION_DATE)
).write.mode(
    "append"
).partitionBy(
    "partition_cd_cliente", "partition_run"
).format(
    "orc"
).save(
    FINAL_POINTERS_PATH_ORC
)
```
