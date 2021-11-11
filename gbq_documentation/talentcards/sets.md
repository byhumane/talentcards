# Tabela - sets

## Descrição geral da tabela

Tabela com dados sobre os cursos/card-sets (*set_id*).

**chave**: *set_id* 

## Descrição das variáveis

* <u>*group_id* (INTEGER)</u>: identificador do grupo, gerado pelo TalentCards
* <u>*set_id* (INTEGER)</u>: identificador do card_set, gerado pelo TalentCards
* <u>*name* (STRING)</u>: nome do card_set (*set_id*)
* <u>*description* (STRING)</u>: descrição do conteúdo do curso/card_set (*set_id*)
* <u>*enable* (BOOLEAN)</u>: true - se o curso está ativo para os usuários; false - caso o curso esteja desativado
* <u>*single_sided* (BOOLEAN)</u>: true - se o card_set (*set_id*) tem apenas um lado; false - caso seja de outros formatos
* <u>*created_at* (STRING)</u>: data de criação do curso/card_set (*set_id*)
* <u>*updated_at* (STRING)</u>: data da última atualização do curso/card_set (*set_id*)


## Processo de criação da tabela

No Cloud Functions, executa o código: https://github.com/byhumane/talentcards/blob/main/cloud_functions/sets_to_landing_zone/main.py

Nesse código, ele consome a API do TalentCards e importa para o Cloud Storage os dados associados aos card_sets.

Em seguida ... \
No Cloud Functions, executa o código: https://github.com/byhumane/talentcards/blob/main/cloud_functions/sets_to_bq/main.py

Agora, ele trata os dados e cria uma tabela no BigQuery com o que foi importado para o Cloud Storage.