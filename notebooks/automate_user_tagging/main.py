from os import sep
from selenium.webdriver import Firefox
from time import sleep
import pandas as pd
from functions import logando, operation_users

# Abrindo url
url = 'https://www.talentcards.com/login'

browser = Firefox()
browser.get(url)

sleep(3)

# Logando na conta e entrando na aba Users
logando(browser, email='jonatas.alves@datamarketplace.com.br', password='')

sleep(1)

# lista como o access_token dos usuarios que terao a tag adicionada
users_table = pd.read_csv('Asasdaestrada.csv')
users = users_table['users'].to_list()

## Adicionando/Removendo as tags aos usuarios

operation_users(browser, users, tag_name='AsasdaEstrada', type_operation='include')
