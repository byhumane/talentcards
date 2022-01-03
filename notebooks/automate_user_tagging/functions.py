from os import error
from selenium.webdriver.common.keys import Keys
from time import sleep
import pandas as pd


def find_element_by_attribute(browser, tag, attribute, attribute_text):
    """
    Encontrar o elemento `type` com `tag` `tag_text`.

    Argumentos:
        - browser = Instancia do browser [firefox, chrome, ...]
        - tag = tipo de elemento que será consultado [input, href, ...]
        - attribute = nome do atributo que vai ser consulada [data-test-id, name, ...]
        - attribute_text = texto do atributo [email-login, password-login, ...]
    """
    elementos = browser.find_elements_by_tag_name(tag)
    for elemento in elementos:
        if attribute_text in elemento.get_attribute(attribute):
            return elemento


def logando(browser, email, password):
    """
    Loga na conta com email `email` e senha `password` e, em seguida, entra na aba Users.

    Argumentos:
        - browser = Instancia do browser [Firefox, Chrome, ...]
        - email = e-mail de um admin [jonatas.alves@datamarketplace.com.br, ...]
        - password = senha para entrar na conta
    """
    email_element = browser.find_element_by_name('email')
    email_element.send_keys(email)

    password_element = browser.find_element_by_name('password')
    password_element.send_keys(password)

    button = browser.find_element_by_id('submit-button-login')
    button.click()

    users_link = find_element_by_attribute(browser, tag='a', attribute='href',
                                           attribute_text='/users')
    users_link.click()


def select_user(browser, access_token):
    """
    Dentro da aba Users, seleciona o buscador e busca pelo usuario `user-name`. Seleciona a opção de edicao.
    
    Atributos:
        - browser = Instancia do browser [Firefox, Chrome, ...]
        - access_token = access_token do usuario, conforme foi escrito no seu cadastro !!! CUIDADO COM ERROS !!!
    """
    search_box = browser.find_element_by_id('input-142')
    search_box.send_keys(access_token)
    sleep(2)

    buttons = browser.find_elements_by_tag_name('button')
    for button in buttons:
        try:
            icon = button.find_element_by_tag_name('i')
        except:
            pass
        if 'pencil' in icon.get_attribute('class'):
            pencil = button
            break
    pencil.click()


def adding_tag(browser, tag_name):
    """
    Adiciona uma tag ao usuário da aba aberta

    Argumentos:
        - browser = Instancia do browser [Firefox, Chrome, ...]
        - tag_name = nome da tag que deve ser adicionada ['Motorista', ...]
    """
    adicionar = True
    spans = browser.find_elements_by_tag_name('span')
    for span in spans:
        if tag_name in span.text:
            adicionar = False
    if adicionar:
        tag_box = browser.find_element_by_id('input-105')
        tag_box.send_keys(tag_name + '\n')
        tag_box.send_keys(Keys.ESCAPE)
        # Salvando alteracao
        sleep(.5)
        buttons = browser.find_elements_by_tag_name('button')
        for button in buttons:
            try:
                if 'Save' in button.text:
                    save_button = button
            except:
                pass
        save_button.click()


def removing_tag(browser, tag_name):
    """
    Remove uma tag ao usuário da aba aberta

    Argumentos:
        - browser = Instancia do browser [Firefox, Chrome, ...]
        - tag_name = nome da tag que deve ser adicionada ['Motorista', ...]
    """
    spans = browser.find_elements_by_tag_name('span')
    for span in spans:
        if tag_name in span.text:
            button = span.find_element_by_tag_name('button')
    button.click()
    # Salvando alteracao
    buttons = browser.find_elements_by_tag_name('button')
    for button in buttons:
        try:
            if 'Save' in button.text:
                save_button = button
        except:
            pass
    save_button.click()


def operation_users(browser, users, tag_name: str, type_operation: str = 'include'):
    """
    Adiciona/Remove uma tag `tag_name` aos usuarios `users` dados.

    Argumentos:
        - browser = Instancia do browser [Firefox, Chrome, ...]
        - users = Lista com o nome dos usuarios ['jonatas de oliveira alves', 'Teste Luana', ...]
        - tag_name = nome da tag que deve ser adicionada ['Motorista', ...]
        - type_operation = tipo de operacao feita ['include', 'exclude']
    """
    # URL da aba users
    url = browser.current_url
    # Lista de usuários que realmente tiveram a tag incluída/excluída
    error_users = {'users': []}
    for user in users:
        # Selecionado um usuario e abrindo a aba de edição
        sleep(1)
        try:
            select_user(browser, user)
        except:
            pass

        sleep(1.5)

        if type_operation == 'include':
            # Adicionando a tag aos usuarios
            try:
                adding_tag(browser, tag_name=tag_name)
            except:
                print(f'--------- O usuário {user} não teve a tag {tag_name} incluída!!! --------------')
                error_users['users'].append(user)
            browser.get(url)
        elif type_operation == 'exclude':
            # Excluindo a tag aos usuarios
            try:
                removing_tag(browser, tag_name=tag_name)
            except:
                print(f'--------- O usuário {user} não teve a tag {tag_name} removida!!! -------------')
                error_users['users'].append(user)
                browser.get(url)
        else:
            print(f'Erro: {type_operation} não é um atributo válido, tente "include" ou "exclude"')
            break

        sleep(1.5)

    pd.DataFrame(error_users).to_csv(f'error_{tag_name}.csv')
