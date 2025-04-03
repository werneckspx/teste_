# Resolução dos Problemas 

Neste teste foram colocadas em prática diversas habilidades, principalmente técnicas, como conhecimento em Python, Banco de Dados e Node.js, além de competências em pesquisa e compreensão para execução da tarefa.

> OBS: Python utilizado na versão 3.13.2, node na versão 22.14, MySQL 8. 

## Organização dos Arquivos

Os arquivos estão organizados da seguinte maneira:

- **webScraping:** resolução do primeiro problema.
- **transformacao:** resolução do segundo problema.
- **bancodedados:** resolução do terceiro problema.
- **api:** resolução do quarto problema.
- **pdf_anexos** e **transform_data:** auxiliares para a resolução.

## 1. Web Scraping

No primeiro código, foi realizado o web scraping de uma página web para obter dois PDFs. Para essa tarefa, foram utilizadas as seguintes bibliotecas e módulos:

- **BeautifulSoup:** para fazer o parse do HTML e extrair os links.
- **Requests:** para realizar a requisição da página.
- **urllib.parse:** para manipulação das URLs.
- **os:** utilizada em todos os códigos para acessar os sistemas e caminhos de arquivos.

## 2. Extração e Transformação de Dados do PDF

No segundo código, realizou-se a extração de dados de um PDF e subsequentes substituições. Para isso, foram utilizadas:

- **pdfplumber:** para ler o arquivo PDF e extrair os dados da tabela de cada página.
- **Pandas:** para converter os dados extraídos em um DataFrame e realizar substituições em alguns valores.
- **zipfile:** para compactar os arquivos em formato ZIP.

## 3. Integração com Banco de Dados (MySQL)

O terceiro código implementa um fluxo de ETL integrando Python e MySQL, envolvendo as seguintes etapas:

- **Conexão com MySQL:** utilizando o módulo `mysql.connector` para conectar ao banco de dados, possibilitando a criação de tabelas e inserção de dados diretamente a partir do Python.
- **Criação de Tabelas:** execução de comandos SQL para criar as tabelas `operadoras` e `despesas`.
- **Manipulação de Dados com Pandas:** leitura e alteração dos dados de arquivos CSV, incluindo a remoção de acentos e tratamento de valores numéricos. Essas alterações foram necessárias devido a problemas na manipulação dos dados e encoding, já que alguns caracteres causavam erros.
- **Carregamento de Dados em Massa:** devido ao grande volume de dados referentes aos trimestres, os arquivos CSV de despesas são processados e carregados via o comando `LOAD DATA LOCAL INFILE`. Esse método otimiza a importação em massa, pois o processamento é realizado diretamente no servidor. Para isso, é necessário executar alguns scripts SQL, como:
  - `SET GLOBAL local_infile = 1;` para ativar a opção na sessão atual.
  - `GRANT FILE ON *.* TO 'seu_usuario'@'localhost'; FLUSH PRIVILEGES;` para dar permissão ao usuário.  
  No código Python, foi incluído o parâmetro `allow_local_infile=True` para habilitar o carregamento local.
  
  No caso dos dados referentes às operadoras, foi realizado um `INSERT` apenas para demonstrar a aplicação do comando; para um projeto otimizado, o ideal seria utilizar o `LOAD DATA LOCAL INFILE`.

- **Consultas SQL:** após o carregamento dos dados, o código executa queries de consulta.

## 4. API com Node e Python

No quarto código, foi implementada uma interface, com as seguintes características:

### Backend 

- **Flask:** utilizado para a criação da API.
- **Requests:** para acessar os dados das requisições HTTP.
- **jsonify:** para converter resultados Python em respostas JSON.
- **CORS:** para possibilitar a comunicação entre o backend e o frontend.
- **mysql.connector:** para a conexão com o MySQL, da mesma forma que no código 3.

O fluxo do backend inicia com a criação da instância do Flask, habilita o CORS para que o front (na porta 8080) possa acessar o backend (na porta 5000), estabelece a conexão com o MySQL e obtém parâmetros da URL. Em seguida, é estabelecido um cursor com dicionário (facilitando a conversão para JSON), determina-se o termo de busca, executa-se a query SQL e, finalmente, o resultado é retornado em formato JSON. Há também um tratamento de erros para eventuais problemas, e a execução do servidor é iniciada.

### Frontend

O componente Vue.js cria uma interface de busca onde o usuário pode digitar um termo (por exemplo, CNPJ). A cada entrada de texto (com mais de 2 caracteres), o frontend realiza uma requisição HTTP GET para o backend (em `http://localhost:5000/search`), enviando o termo como parâmetro (`q`). O backend consulta o MySQL e retorna os resultados em JSON, os quais são exibidos em cards formatados ou, se não houver resultados, é apresentada a mensagem "nenhum resultado". Durante a requisição, são exibidos estados de carregamento, e toda a comunicação é realizada via Axios.


