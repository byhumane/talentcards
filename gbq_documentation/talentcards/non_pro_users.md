# Tabela - non_pro_users

## Descrição geral da tabela

Tabela de usuários que não são consumidores. \
**Exemplo**: funcionários Humane; funcionários Danone.

**chave**: *user_id*

## Descrição das variáveis

* <u>*user_id* (INTEGER)</u>: identificador de indivíduo, gerado pelo TalentCards
* <u>*association* (STRING)</u>: grupo a qual o indivíduo pertente (Humane; Danone; etc)

## Processo de criação da tabela

Executa o código: https://github.com/byhumane/talentcards/tree/main/notebooks/ 2.1_Prepare non_pro_user.ipynb

Nesse código, ele incorpora uma lista de usuários dada em: https://github.com/byhumane/talentcards/blob/main/params/non_professional_users.csv

Em seguida, ele funde esses dados com a tabela **dtm_engagement.dim_users** e gera a nossa tabela, exportando ela para o BigQuery.