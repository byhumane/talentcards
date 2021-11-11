# Query - ft_users_engagement

## Descrição geral da query

Tabela com atividade dos usuários por dia, desde o início do projeto. 

Os dados são filtrados para os casos em que *group_id=1818* e o indivíduo é realmente um usuário e não um funcionário da Humane, Danone, etc.

**chave**: *user_id, action_date*


## Descrição das variáveis

* <u>*action_date* (STRING)</u>: data em que foi registrada as atividades do usuário (*user_id*)
* <u>*user_id* (INTEGER)</u>: identificador do usuário
* <u>*user_name* (STRING)</u>: nome do usuário (*user_id*)
* <u>*h_group_id* (INTEGER)</u>: identificador de grupos do Byhumane (não os mesmos grupos do TalentCards)
* <u>*created_at* (TIMESTAMP)</u>: data e hora da criação da conta (*user_id, group_id=1818*)
* <u>*days_since_last_login* (FLOAT)</u>: número de dias desde a última vez que logou na conta
* <u>*last_start_date* (TIMESTAMP)</u>: data da última vez que iniciou um card_set
* <u>*timedelta_since_last_start* (STRING)</u>: diferença de tempo desde a última vez que iniciou um card_set
* <u>*days_sisce_last_start* (FLOAT)</u>: dias desde a última vez que iniciou um card_set
* <u>*last_completion_date* (TIMESTAMP)</u>: data da última vez que completou um card_set
* <u>*timedelta_since_last_completion* (STRING)</u>: diferença de tempo desde a última vez que completou um card_set
* <u>*days_since_last_completion* (FLOAT)</u>: dias desde a última vez que completou um card_set
* <u>*nb_of_completed_sets* (INTEGER)</u>: número de card_sets completados nos últimos sete dias
* <u>*user_status* (STRING)</u>: \
    0.stone: conta inativa (*enable*=False)\
    1.bird: ativo, mas nunca logou na conta\
    2.missing: ativo e não loga a mais de sete dias\
    3.curious: apenas logou na conta nos últimos sete dias\
    4.consumer: apenas iniciou um card_set nos últimos sete dias\
    5.learner: completou pelo menos um card_set nos últimos sete dias



## SQL

Ultima atualização: 09/11/2021

~~~~sql
SELECT
  engagement.action_date,
  engagement.user_id,
  user_name,
  users.h_group_id,
  engagement.created_at,
  engagement.days_since_last_login,
  engagement.last_start_date,
  engagement.timedelta_since_last_start,
  engagement.days_since_last_start,
  engagement.last_completion_date,
  engagement.timedelta_since_last_completion,
  engagement.days_since_last_completion,
  engagement.nb_of_completed_sets,
  engagement.user_status
FROM
  `analytics-dev-308300.raw_engagement.users_engagement` as engagement LEFT JOIN `analytics-dev-308300.dtm_engagement.dim_users` as users USING (user_id)
WHERE
  association is null