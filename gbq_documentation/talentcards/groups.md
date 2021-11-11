# Tabela - groups

## Descrição geral da tabela

Tabela com os grupos de contas do TalentCards. Toda conta no TalentCards está associada a um idivíduo e grupo (*group_id*).

**chave**: *group_id*

## Descrição das variáveis

* <u>*group_id* (INTEGER)</u>: identificador do grupo, gerado pelo TalentCards
* <u>*name* (STRING)</u>: nome do grupo (Humane; Danone & Humane; etc)
* <u>*leader_monthly* (INTEGER)</u>: *user_id* do indivíduo que lidera ??? no mês
* <u>*leader_all* (INTEGER)</u>: *user_id* do indivíduo que lidera ??? no geral
* <u>*created_at* (STRING)</u>: data de criação do grupo
* <u>*updated_at* (STRING)</u>: data de atualização do grupo


## Processo de criação da tabela

Executa o código: https://us-central1-analytics-dev-308300.cloudfunctions.net/talentcard-groups-to-landing-zone

Nesse código, ele consome a API do TalentCards e importa para o Cloud Storage os dados associados aos grupos.

Em seguida ... \
Executa o código: https://us-central1-analytics-dev-308300.cloudfunctions.net/talentcard-groups-to-bq

Agora, ele cria uma tabela no BigQuery com o que foi importado para o Cloud Storage.