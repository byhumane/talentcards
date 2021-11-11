# Query - dim_groups

## Descrição geral da query

Gera uma tabela com os grupos de contas do TalentCards. Toda conta no TalentCards está associada a um idivíduo e grupo (*group_id*). Uma cópia exata da tabela "talentcards.groups".

**chave**: *group_id*

## Descrição das variáveis

* <u>*group_id* (INTEGER)</u>: identificador do grupo, gerado pelo TalentCards
* <u>*name* (STRING)</u>: nome do grupo (Humane; Danone & Humane; etc)
* <u>*leader_monthly* (INTEGER)</u>: *user_id* do indivíduo que lidera ??? no mês
* <u>*leader_all* (INTEGER)</u>: *user_id* do indivíduo que lidera ??? no geral
* <u>*created_at* (STRING)</u>: data de criação do grupo
* <u>*updated_at* (STRING)</u>: data de atualização do grupo


## SQL

Ultima atualização: 08/11/2021

~~~~sql
SELECT * FROM `analytics-dev-308300.talentcards.groups`
