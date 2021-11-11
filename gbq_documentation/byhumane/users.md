# Tabela - users

## Descrição geral da tabela

Tabela com dados pessoais (*email, name*, etc) e da conta (*talent_id, group_id*).

**chave**: *id*

## Descrição das variáveis

* <u>*id* (INTEGER)</u>: identificador de indivíduo, gerado pelo Byhumane
* <u>*email* (STRING)</u>: e-mail do usuário
* <u>*encrypted_password* (STRING)</u>: senha criptografada
* <u>*reset_password_token* (STRING)</u>: ???
* <u>*reset_password_sent_at* (STRING)</u>: ???
* <u>*remember_created_at* (STRING)</u>: ???
* <u>*created_at* (STRING)</u>: data de criação da conta
* <u>*updated_at* (STRING)</u>: data da última atualização da conta
* <u>*name* (STRING)</u>: nome do usuário (não completo)
* <u>*talent_id* (INTEGER)</u>: identificador de indivíduo, gerado pelo TalentCards
* <u>*token* (STRING)</u>: token que permite o acesso do indivíduo à uma conta
* <u>*last_talent_update* (STRING)</u>: ???
* <u>*group_id* (INTEGER)</u>: identificador de grupos do Byhumane (não os mesmos grupos do TalentCards)
* <u>*location_id* (INTEGER)</u>: ??? identificado de localização do indivíduo ???

## Processo de criação da tabela

No Cloud Functions, executa o código: https://github.com/byhumane/byhumane/blob/main/google_cloud_functions/byhumane_to_gbq/main.py

Nesse código, ele importa dados do Heroku, que, por sua vez, importam dados do site do byhumane. Em seguida, ele cria tabelas, de modo que, uma delas é esta tabela.