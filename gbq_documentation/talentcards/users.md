# Tabela - users

## Descrição geral da tabela

Tabela com dados pessoais (*email, first_name*, etc) e da conta (*user_id, group_id*) extraídos em determinadas datas (*extraction_timestamp*). 

Basicamente, uma foto dos dados de uma conta em determinados momentos.

**chave**: ???

## Descrição das variáveis

* <u>*user_id* (INTEGER)</u>: identificador de indivíduo, gerado pelo TalentCards
* <u>*group_id* (INTEGER)</u>: identificador do grupo, gerado pelo TalentCards
* <u>*access_token* (STRING)</u>: cada usuário (*user_id*) recebe um token para cada grupo (*group_id*) em que está inseridoa e cada token serve como o login de uma conta
* <u>*email* (STRING)</u>: e-mail do usuário (*user_id*)
* <u>*first_name* (STRING)</u>: nome do usuário (*user_id*)
* <u>*last_name* (STRING)</u>: sobrenome do usuário (*user_id*)
* <u>*mobile* (STRING)</u>: número de celular
* <u>*created_at* (STRING)</u>: data de criação da conta (*user_id, group_id*)
* <u>*last_login* (STRING)</u>: data da última vez que logou na conta
* <u>*extraction_timestamp* (STRING)</u>: data em que esses dados foram extraídos do TalentCards
* <u>*updated_at* (STRING)</u>: data da última atualização antes da extração dos dados
* <u>*enable* (BOOLEAN)</u>: true - se a conta está ativa; false - caso a conta esteja desativada

## Processo de criação da tabela

No Cloud Functions, executa o código: https://github.com/byhumane/talentcards/blob/main/cloud_functions/users_to_landing_zone/main.py

Nesse código, ele consome a API do TalentCards e importa para o Cloud Storage os dados associados às contas dos usuários.

Em seguida ... \
No Cloud Functions, executa o código: https://github.com/byhumane/talentcards/blob/main/cloud_functions/users_to_bq/main.py

Agora, ele trata os dados e cria uma tabela no BigQuery com o que foi importado para o Cloud Storage.