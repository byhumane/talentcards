# Query - dim_sets

## Descrição geral da query

Gera uma tabela com dados sobre os cursos/card-sets (*set_id*). É uma cópia exata do "talentcards.sets"

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


## SQL

Ultima atualização: 08/11/2021

~~~~sql
SELECT * FROM `analytics-dev-308300.talentcards.sets`

