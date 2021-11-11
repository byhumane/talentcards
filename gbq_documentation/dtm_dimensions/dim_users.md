# Query - dim_users

## Descrição geral da query

Gera uma tabela com os dados mais atuais de uma conta e seu usuário, apresentando dados pessoais (*email, user_name*, etc) e da conta (*user_id, group_id, acces_token*)

**chave**: *user_id, group_id*

## Descrição das variáveis

* <u>*user_id* (INTEGER)</u>: identificador de indivíduo, gerado pelo TalentCards
* <u>*h_user_id* (INTEGER)</u>: identificador de indivíduo, gerado pelo Byhumane
* <u>*group_id* (INTEGER)</u>: identificador do grupo, gerado pelo TalentCards
* <u>*h_group_id* (INTEGER)</u>: identificador de grupos do Byhumane (não os mesmos grupos do TalentCards)
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
    SELECT
        DISTINCT users.user_id,
        byhumane_users.id as h_user_id,
        users.group_id,
        byhumane_users.group_id as h_group_id,
        users.access_token,
        users.email,
        CONCAT (first_name, " ", last_name) AS user_name,
        users.mobile,
        users.enabled,
        non_pro_users.association,
        CAST(users.created_at AS TIMESTAMP) AS created_at,
        CAST(users.updated_at AS TIMESTAMP) AS updated_at,
        CAST (users.last_login AS TIMESTAMP) AS last_login,
        LEFT(users.last_login,10) AS last_login_date,
        DATE_DIFF(CAST(users.extraction_timestamp AS TIMESTAMP),CAST (users.last_login AS TIMESTAMP),DAY) AS days_since_last_login,
        CAST(users.extraction_timestamp AS TIMESTAMP) AS extraction_timestamp,
        LEFT(users.extraction_timestamp,10) AS extraction_date
    FROM
        `analytics-dev-308300.talentcards.users` AS users
    LEFT JOIN
        `analytics-dev-308300.talentcards.non_pro_users` AS non_pro_users
    ON
        users.user_id=non_pro_users.user_id
    LEFT JOIN byhumane.users as byhumane_users ON users.user_id=byhumane_users.talent_id
    WHERE
        extraction_timestamp = (
        SELECT
            MAX(extraction_timestamp)
        FROM
            `analytics-dev-308300.talentcards.users`)
