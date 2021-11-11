# Tabela - user_engagement

## Descrição geral da tabela

Tabela com atividade dos usuários por dia, desde o início do projeto. 

Os dados são filtrados para os casos em que *group_id=1818* e o indivíduo é realmente um usuário e não um funcionário da Humane, Danone, etc.

**chave**: *user_id, action_date*

## Descrição das variáveis

* <u>*action_date* (STRING)</u>: data em que foi registrada as atividades do usuário (*user_id*)
* <u>*user_id* (INTEGER)</u>: identificador do usuário
* <u>*created_at* (TIMESTAMP)</u>: data e hora da criação da conta (*user_id, group_id=1818*)
* <u>*days_since_last_login* (FLOAT)</u>: número de dias desde a última vez que logou na conta
* <u>*enabled* (STRING)</u>: True - se a conta está ativa e False - se a conta estiver inativa
* <u>*last_start_date* (TIMESTAMP)</u>: data da última vez que iniciou um card_set
* <u>*timedelta_since_last_start* (STRING)</u>: diferença de tempo desde a última vez que iniciou um card_set
* <u>*days_sisce_last_start* (FLOAT)</u>: dias desde a última vez que iniciou um card_set
* <u>*last_completion_date* (TIMESTAMP)</u>: data da última vez que completou um card_set
* <u>*timedelta_since_last_completion* (STRING)</u>: diferença de tempo desde a última vez que completou um card_set
* <u>*days_since_last_completion* (FLOAT)</u>: dias desde a última vez que completou um card_set
* <u>*nb_of_completed_sets* (INTEGER)</u>: número de card_sets completados no dia
* <u>*user_status* (STRING)</u>: \
    0.stone: conta inativa (*enable*=False)\
    1.bird: ativo, mas nunca logou na conta\
    2.missing: ativo e não loga a mais de sete dias\
    3.curious: apenas logou na conta nos últimos sete dias\
    4.consumer: apenas iniciou um card_set nos últimos sete dias\
    5.learner: completou pelo menos um card_set nos últimos sete dias


## Processo de criação da tabela

No Cloud Functions, executa o código: https://github.com/byhumane/talentcards/blob/main/cloud_functions/users_engagement_to_bq/main.py

Nesse código, ele faz algumas queries em "dtm_engagement.dim_users", "dtm_engagement.hist_users" e "dtm_engagement.ft_content_consumption". Em seguida, constroi uma tabela com a atividade de cada usuário em todos os dias, desde o início do projeto (16/08/2021). 