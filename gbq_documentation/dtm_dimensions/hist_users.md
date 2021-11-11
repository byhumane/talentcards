# Query - hist_users

## Descrição geral da query

Gera uma tabela com dados pessoais (*email, user_name*, etc) e da conta (*user_id, group_id*) extraídos em determinadas datas e hora (*extraction_timestamp*).

**chave**: *user_id, group_id, extraction_timestamp*

## Descrição das variáveis

* <u>*user_id* (INTEGER)</u>: identificador de indivíduo, gerado pelo TalentCards
* <u>*group_id* (INTEGER)</u>: identificador do grupo, gerado pelo TalentCards
* <u>*access_token* (STRING)</u>: cada usuário (*user_id*) recebe um token para cada grupo (*group_id*) em que está inseridoa e cada token serve como o login de uma conta
* <u>*email* (STRING)</u>: e-mail do usuário (*user_id*)
* <u>*user_name* (STRING)</u>: nome do usuário (*user_id*)
* <u>*mobile* (STRING)</u>: número de celular
* <u>*enabled* (BOOLEAN)</u>: true - se a conta está ativa; false - caso a conta esteja desativada
* <u>*association* (STRING)</u>: grupo a qual o indivíduo pertente (Humane; Danone; etc)
* <u>*created_at* (TIMESTAMP)</u>: data e hora da criação da conta (*user_id, group_id*)
* <u>*updated_at* (TIMESTAMP)</u>: data e hora da última atualização dos dados
* <u>*last_login* (TIMESTAMP)</u>: data e hora da última vez que logou na conta
* <u>*last_login_date* (STRING)</u>: apenas a data da última vez que logou na conta
* <u>*days_since_last_login* (INTEGER)</u>: dias desde a última vez que logou na conta
* <u>*extraction_timestamp* (TIMESTAMP)</u>: data e hora em que esses dados foram extraídos do TalentCards 
* <u>*extraction_date* (STRING)</u>: apenas data em que esses dados foram extraídos do TalentCards


## SQL

Ultima atualização: 08/11/2021

~~~~sql
SELECT DISTINCT
  users.user_id,
  group_id,
  access_token,
  email,
  CONCAT (first_name, " ", last_name) as user_name,
  mobile,
  enabled,
  association,
  CAST(created_at as TIMESTAMP) as created_at,
  CAST(updated_at AS TIMESTAMP) as updated_at,
  CAST (last_login AS TIMESTAMP) as last_login,
  LEFT(last_login,10) as last_login_date,
  DATE_DIFF(CAST(extraction_timestamp AS TIMESTAMP),CAST (last_login AS TIMESTAMP),DAY) AS days_since_last_login,
  CAST(extraction_timestamp AS TIMESTAMP) as extraction_timestamp,
  LEFT(extraction_timestamp,10) as extraction_date
FROM
  `analytics-dev-308300.talentcards.users` as users LEFT JOIN `analytics-dev-308300.talentcards.non_pro_users` as non_pro_users ON users.user_id=non_pro_users.user_id
