# Tabela - activity_dones

## Descrição geral da tabela

Tabela com as notas e comentários feitos pelos usuŕios no Byhumane.

**chave**: *id*

## Descrição das variáveis

* <u>*id* (INTEGER)</u>: identificador do relatório (nota + comentário), gerado por usuários no Byhymane
* <u>*activity_id* (INTEGER)</u>: identificador de card_set, gerado pelo Byhumane (id da tabela "activities")
* <u>*xp_points* (FLOAT)</u>: ???
* <u>*expertise_points* (FLOAT)</u>: ???
* <u>*user_id* (INTEGER)</u>: identificador do indivíduo, gerado pelo Byhumane
* <u>*created_at* (STRING)</u>: data em que foi dada a nota e feito o comentário
* <u>*updated_at* (STRING)</u>: data da última atualização da nota e comentário do usuário (*user_id*)
* <u>*rating* (FLOAT)</u>: nota dada pelo usuário (0 - 5)
* <u>*comment* (STRING)</u>: comentário feito pelo usuário (*user_id*)
* <u>*completed_at* (STRING)</u>: ???
* <u>*released* (STRING)</u>: ???


## Processo de criação da tabela

No Cloud Functions, executa o código: https://github.com/byhumane/byhumane/blob/main/google_cloud_functions/byhumane_to_gbq/main.py

Nesse código, ele importa dados do Heroku, que, por sua vez, importam dados do site do byhumane. Em seguida, ele cria tabelas, de modo que, uma delas é esta tabela.