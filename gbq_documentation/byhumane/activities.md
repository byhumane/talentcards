# Tabela - activities

## Descrição geral da tabela

Tabela com os dados dos card_sets extraídos do Byhumane.

**chave**: *id*

## Descrição das variáveis

* <u>*id* (INTEGER)</u>: identificador de card_set, gerado pelo Byhumane
* <u>*name* (STRING)</u>: nome do card_set (nome não muito confiável)
* <u>*created_at* (STRING)</u>: data de criação do card_set
* <u>*updated_at* (STRING)</u>: data da última atualização do card_set
* <u>*xp_points* (INTEGER)</u>: pontos de experiência a serem ganhos
* <u>*expertise_points* (FLOAT)</u>: pontos de expertise a serem ganhos
* <u>*talent_id* (INTEGER)</u>: identificador de card_set (equivalente ao set_id), gerado pelo TalentCards 
* <u>*topic_id* (FLOAT)</u>: ???
* <u>*rating* (STRING)</u>: ???


## Processo de criação da tabela

No Cloud Functions, executa o código: https://github.com/byhumane/byhumane/blob/main/google_cloud_functions/byhumane_to_gbq/main.py

Nesse código, ele importa dados do Heroku, que, por sua vez, importam dados do site do byhumane. Em seguida, ele cria tabelas, de modo que, uma delas é esta tabela.