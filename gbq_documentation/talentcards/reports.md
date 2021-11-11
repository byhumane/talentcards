# Tabela - reports

## Descrição geral da tabela

Tabela com o status de uma conta (*user_id*, *group_id*) referente a um card_set (set_id) numa 
determinada data (*extraction_timestamp*), em que os dados foram extraídos.

Basicamente, é uma foto do status atual de uma conta num determinado curso.

**chaves**: ???

## Descrição das variáveis

* <u>*group_id* (INTEGER)</u>: identificador do grupo, gerado pelo TalentCards
* <u>*user_id* (INTEGER)</u>: identificador de indivíduo, gerado pelo TalentCards
* <u>*sequence_id* (FLOAT)</u>: identificador da sequência/conjunto de card_sets em que este cardset (*set_id*) está inserido
* <u>*set_id* (INTEGER)</u>: identificador de cursos do TalentCards
* <u>*set_tests* (INTEGER)</u>: quantidade de conjunto de testes
* <u>*finished_tests* (INTEGER)</u>: número de testes concluídos
* <u>*progress* (FLOAT)</u>: porcentagem de cards concluídos
* <u>*cards* (INTEGER)</u>: número de cards
* <u>*tests* (INTEGER)</u>: quantidade de testes do card_set
* <u>*started_at* (STRING)</u>: data de início do card_set nessa conta (*user_id, group_id*)
* <u>*completed_at* (STRING)</u>: data de conclusão do card_set
* <u>*extraction_timestamp* (STRING)</u>: data em que esses dados foram extraídos do TalentCards

## Processo de criação da tabela

No Cloud Functions, executa o código: https://github.com/byhumane/talentcards/blob/main/cloud_functions/reports_to_landing_zone/main.py

Nesse código, ele consome a API do TalentCards e importa para o Cloud Storage os dados associados aos reports.

Em seguida ... \
No Cloud Functions, executa o código: https://github.com/byhumane/talentcards/blob/main/cloud_functions/reports_to_bq/main.py

Agora, ele trata os dados e cria uma tabela no BigQuery com o que foi importado para o Cloud Storage.