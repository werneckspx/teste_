import mysql.connector
from zipfile import ZipFile
import csv
import os
import unicodedata
import pandas as pd

def remove_acentos(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto)
    texto = unicodedata.normalize('NFD', texto)\
                       .encode('ascii', 'ignore')\
                       .decode('utf-8')
    texto = ''.join(c for c in texto if c.isalnum() or c in ('@', '.', '-', '_', ' '))
    return texto

# Conectar ao MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="270211Tfj",
    database="dados",
    allow_local_infile=True 
)
cursor = conn.cursor()

# Criar tabela 'operadoras'
cursor.execute('''
    CREATE TABLE operadoras (
        Registro_ANS VARCHAR(20) PRIMARY KEY,
        CNPJ VARCHAR(14),
        Razao_Social VARCHAR(255),
        Nome_Fantasia VARCHAR(255),
        Modalidade VARCHAR(100),
        Logradouro VARCHAR(255),
        Numero VARCHAR(100),
        Complemento VARCHAR(100),
        Bairro VARCHAR(100),
        Cidade VARCHAR(100),
        UF CHAR(2),
        CEP VARCHAR(8),
        DDD VARCHAR(4),
        Telefone VARCHAR(100),
        Fax VARCHAR(20),
        Endereco_eletronico VARCHAR(255),
        Representante VARCHAR(255),
        Cargo_Representante VARCHAR(100),
        Regiao_de_Comercializacao VARCHAR(100),
        Data_Registro_ANS DATE
    );
''')

# Criar tabela 'despesas'
cursor.execute('''
    CREATE TABLE despesas (
        id INT AUTO_INCREMENT PRIMARY KEY,
        DATA DATE,
        REG_ANS VARCHAR(20),
        CD_CONTA_CONTABIL VARCHAR(20),
        DESCRICAO VARCHAR(255),
        VL_SALDO_INICIAL DECIMAL(15,2),
        VL_SALDO_FINAL DECIMAL(15,2),
        QUARTER VARCHAR(10),
        ANO VARCHAR(5),
        FOREIGN KEY (REG_ANS) REFERENCES operadoras(Registro_ANS)
    );
''')
conn.commit()

# Define caminhos relativos a partir da pasta
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
data_dir = os.path.join(base_dir, "bancodedados", "data")

# Processar o Relatorio_cadop.csv
relatorio_csv = os.path.join(data_dir, "Relatorio_cadop.csv")
df = pd.read_csv(relatorio_csv, sep=';', encoding='utf-8')

# Aplica a limpeza
df_sanitizado = df.map(lambda x: remove_acentos(x) if isinstance(x, str) else x)

# Salva o novo arquivo
relatorio_alterado_csv = os.path.join(data_dir, "relatorio_alterado.csv")
df_sanitizado.to_csv(relatorio_alterado_csv, sep=';', index=False, encoding='utf-8')

with open(relatorio_alterado_csv, 'r', encoding='utf-8') as file:
    csv_data = csv.reader(file, delimiter=';')
    next(csv_data) 
    for row in csv_data:
        cursor.execute('''
        INSERT INTO operadoras 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', row)
conn.commit()

arquivos = [
    '1T2024.csv', '2T2024.csv', '3T2024.csv', '4T2024.csv'
]

cursor.execute("ALTER TABLE despesas DISABLE KEYS")
conn.autocommit = False

for arquivo in arquivos:
    nome_arquivo = arquivo.replace('.csv', '')
    quarter = int(nome_arquivo[0])
    ANO = int(nome_arquivo[2:6])
    caminho_completo = os.path.join(data_dir, arquivo)
    
    df = pd.read_csv(caminho_completo, sep=';', dtype=str)
    df_sanitizado = df.map(remove_acentos)
    
    # Processar colunas numéricas
    df_sanitizado.iloc[:, 4] = df_sanitizado.iloc[:, 4].str.replace('.', '', regex=False).str.replace(',', '.').astype(float)
    df_sanitizado.iloc[:, 5] = df_sanitizado.iloc[:, 5].str.replace('.', '', regex=False).str.replace(',', '.').astype(float)
    
    # Adicionar quarter e ano ao DataFrame
    df_sanitizado['QUARTER'] = quarter
    df_sanitizado['ANO'] = ANO
    
    # Salvar em um CSV temporário
    temp_csv = os.path.join(data_dir, f"temp_{arquivo}")
    df_sanitizado.to_csv(temp_csv, index=False, sep=';')
    
    # Converter o caminho para o formato aceito pelo MySQL (usar barras normais)
    temp_csv_abs = os.path.abspath(temp_csv).replace("\\", "/")
    
    # Carregar via LOAD DATA LOCAL INFILE
    cursor.execute(f'''
        LOAD DATA LOCAL INFILE '{temp_csv_abs}'
        INTO TABLE despesas
        FIELDS TERMINATED BY ';'
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS
        (DATA, REG_ANS, CD_CONTA_CONTABIL, DESCRICAO, 
         VL_SALDO_INICIAL, VL_SALDO_FINAL, QUARTER, ANO)
    ''')
    os.remove(temp_csv)  # Limpar arquivo temporário

cursor.execute("ALTER TABLE despesas ENABLE KEYS")
conn.commit()

cursor.execute('''
    SELECT 
        o.Razao_Social, 
        SUM(d.VL_SALDO_FINAL - d.VL_SALDO_INICIAL) AS Total_Despesas
    FROM despesas d
    JOIN operadoras o ON d.REG_ANS = o.Registro_ANS
    WHERE 
        d.DESCRICAO LIKE '%EVENTOS SINISTROS CONHECIDOS OU AVISADOS  DE ASSISTENCIA A SAUDE MEDICO HOSPITALAR%'
        AND d.ANO = 2024
        AND d.QUARTER = 4
    GROUP BY o.Razao_Social
    ORDER BY Total_Despesas DESC
    LIMIT 10;
''')
print("Top 10 Operadoras (Último Trimestre):")
for result in cursor.fetchall():
    print(result)
    
cursor.execute('''
    SELECT 
        o.Razao_Social, 
        SUM(d.VL_SALDO_FINAL - d.VL_SALDO_INICIAL) AS Total_Despesas
    FROM despesas d
    JOIN operadoras o ON d.REG_ANS = o.Registro_ANS
    WHERE 
        d.DESCRICAO LIKE '%EVENTOS SINISTROS CONHECIDOS OU AVISADOS  DE ASSISTENCIA A SAUDE MEDICO HOSPITALAR%'
        AND d.ANO = (SELECT MAX(ANO) FROM despesas)
    GROUP BY o.Razao_Social
    ORDER BY Total_Despesas DESC
    LIMIT 10
''')
print("\nTop 10 Operadoras (Último Ano):")
for result in cursor.fetchall():
    print(result)

cursor.close()
conn.close()