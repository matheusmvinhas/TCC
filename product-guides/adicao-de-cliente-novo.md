# 🆕 Adição de cliente novo

Olhando o processo pelo lado de dados, para que o cliente entrar no fluxo de geração de dados precisará:

* Contrato ativo no V2
*   Presente no csv client\_list que está no S3 :

    ```python
    s3://ip-byte-pool/infoprice/curated/ftp/clients_list/
    ```
*   Presente no json que está no repositório do Corleone:&#x20;

    ```json
    configs/tasks_helper_files/data_load/infopanel_to_api_disaggregated_clients.json
    ```



Uma vez cadastro, o cliente entra no fluxo de geração e LOAD da tabela no banco. Caso o contrato do cliente expire, ele deixará de gerar dados para o cliente.&#x20;
