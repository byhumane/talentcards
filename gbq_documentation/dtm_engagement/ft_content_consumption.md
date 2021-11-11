# Query - ft_content_consumption

## Descrição geral da query

Gera uma tabela com o status de cada card_set (*set_id*) iniciado por uma conta (*user_id, group_id*).

**chave**: *user_id, group_id, set_id* 

## Descrição das variáveis

* <u>*group_id* (INTEGER)</u>: identificador do grupo, gerado pelo TalentCards
* <u>*user_id* (INTEGER)</u>: identificador do usuário, gerado pelo TalentCards
* <u>*user_name* (STRING)</u>: nome do usuário (*user_id*)
* <u>*h_group_id* (INTEGER)</u>: identificador de grupos do Byhumane (não os mesmos grupos do TalentCards)
* <u>*set_id* (INTEGER)</u>: identificador do card_set, gerado pelo TalentCards
* <u>*set_tests* (INTEGER)</u>: quantidade de conjunto de testes
* <u>*finished_tests* (INTEGER)</u>: número de testes concluídos
* <u>*progress* (FLOAT)</u>: porcentagem de cards concluídos
* <u>*cards* (INTEGER)</u>: número de cards
* <u>*tests* (INTEGER)</u>: quantidade de testes do card_set
* <u>*started_at* (DATETIME)</u>: data de início do card_set nessa conta (*user_id, group_id*)
* <u>*completed_at* (DATETIME)</u>: data de conclusão do card_set
* <u>*consumption_duration* (INTEGER)</u>: segundos entre o início do card_set (set_id) e sua conclusão
* <u>*consumption_duration_minutes* (FLOAT)</u>: minutos entre o início do card_set (set_id) e sua conclusão


## SQL

Ultima atualização: 08/11/2021

~~~~sql
with
     no_repetition_report as (
        SELECT DISTINCT
            group_id,
            user_id,
            set_id,
            set_tests,
            finished_tests,
            progress,
            cards,
            tests,
            started_at,
            completed_at,
        FROM talentcards.reports
    ),

    completed as (
        SELECT
            group_id,
                user_id,
                set_id,
                set_tests,
                finished_tests,
                progress,
                cards,
                tests,
                started_at,
                completed_at,
        FROM (
        SELECT DISTINCT
            group_id,
            set_id,
            user_id,
            LAST_VALUE(set_tests) over (w1) as set_tests,
            LAST_VALUE(finished_tests) over (w1) as finished_tests,
            LAST_VALUE(progress) over (w1) as progress,
            LAST_VALUE(cards) over (w1) as cards,
            LAST_VALUE(tests) over (w1) as tests,
            LAST_VALUE(started_at) over (w1) as started_at,
            LAST_VALUE(completed_at) over (w1) as completed_at,
        FROM no_repetition_report
        WHERE completed_at is not null
        WINDOW w1 AS (
        PARTITION BY
            group_id,
            set_id,
            user_id
        ORDER BY completed_at ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        )
    ),

    started as (
        SELECT
            group_id,
            user_id,
            set_id,
            set_tests,
            finished_tests,
            progress,
            cards,
            tests,
            started_at,
            completed_at,
        FROM (
            SELECT DISTINCT
                group_id,
                set_id,
                user_id,
                LAST_VALUE(set_tests) over (w1) as set_tests,
                LAST_VALUE(finished_tests) over (w1) as finished_tests,
                LAST_VALUE(progress) over (w1) as progress,
                LAST_VALUE(cards) over (w1) as cards,
                LAST_VALUE(tests) over (w1) as tests,
                LAST_VALUE(started_at) over (w1) as started_at,
                LAST_VALUE(completed_at) over (w1) as completed_at,
            FROM no_repetition_report
            WHERE
                  completed_at is null
              AND started_at is not null
            WINDOW w1 AS (
            PARTITION BY
                group_id,
                set_id,
                user_id
            ORDER BY completed_at ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            )
    ),

     reports as (
         SELECT * FROM completed
         UNION ALL
         SELECT * FROM started)

SELECT
    CAST(reports.group_id AS INT64) as group_id,
    reports.user_id,
    user_name,
    users.h_group_id,
    set_id,
    set_tests,
    finished_tests,
    progress,
    cards,
    tests,
    CAST(TIMESTAMP(started_at) AS DATETIME) AS started_at,
    CAST(TIMESTAMP(completed_at) AS DATETIME) as completed_at,
    TIMESTAMP_DIFF(TIMESTAMP(completed_at), TIMESTAMP(started_at), SECOND) as consumption_duration,
    TIMESTAMP_DIFF(TIMESTAMP(completed_at), TIMESTAMP(started_at), SECOND)/60 as consumption_duration_minutes
FROM
    reports LEFT JOIN `analytics-dev-308300.dtm_engagement.dim_users` as users USING (user_id)
WHERE
    association is null
ORDER BY
    completed_at DESC,started_at DESC