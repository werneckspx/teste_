import os
import logging
import unicodedata
from typing import Any
from zipfile import ZipFile

import pandas as pd
import mysql.connector
import requests

#logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Config:
    """Configurações da aplicação"""
    
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    DATA_DIR = os.path.join(BASE_DIR, "bancodedados", "data")
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': os.getenv('DB_PASSWORD', '270211Tfj'),
        'database': 'dados',
        'allow_local_infile': True
    }
    URLS = {
        'operadoras': 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv',
        'despesas_base': 'https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/'
    }

def setup_directories() -> None:
    """Cria a estrutura de diretórios"""
    
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    logger.info(f"Diretório de dados criado em: {Config.DATA_DIR}")

def download_and_extract(url: str, file_name: str) -> None:
    """Baixa e extrai arquivos remotos"""
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        file_path = os.path.join(Config.DATA_DIR, file_name)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        if file_name.endswith('.zip'):
            with ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(Config.DATA_DIR)
            os.remove(file_path)
            logger.info(f"Arquivo {file_name} extraído com sucesso")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao baixar {url}: {str(e)}")
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")

def sanitize_data(texto: Any) -> str:
    """Remove acentos e caracteres especiais de textos"""
    
    if pd.isna(texto):
        return ''
    
    texto = str(texto)
    texto = unicodedata.normalize('NFD', texto)\
                       .encode('ascii', 'ignore')\
                       .decode('utf-8')
    return ''.join(c for c in texto if c.isalnum() or c in ('@', '.', '-', '_', ' '))

def create_tables(conn: mysql.connector.MySQLConnection) -> None:
    """Cria as tabelas no banco de dados"""
    
    queries = [
        """
        CREATE TABLE IF NOT EXISTS operadoras (
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
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS despesas (
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
        )
        """
    ]
    
    try:
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
        conn.commit()
        logger.info("Tabelas criadas com sucesso")
        
    except mysql.connector.Error as err:
        logger.error(f"Erro ao criar tabelas: {err}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()

def process_operadoras(conn: mysql.connector.MySQLConnection) -> None:
    """Processa e importa dados das operadoras"""
    
    file_path = os.path.join(Config.DATA_DIR, "Relatorio_cadop.csv")
    try:
        df = pd.read_csv(file_path, sep=';', dtype=str)
        df = df.map(sanitize_data)
        
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                columns = ', '.join(row.index)
                query = f"INSERT INTO operadoras ({columns}) VALUES ({placeholders})"
                cursor.execute(query, tuple(row.values))
            conn.commit()
        logger.info(f"Dados de operadoras importados: {len(df)} registros")
        
    except Exception as e:
        logger.error(f"Erro ao processar operadoras: {str(e)}")
        conn.rollback()

def process_despesas(conn: mysql.connector.MySQLConnection) -> None:
    """Processa e importa dados de despesas"""
    
    try:
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE despesas DISABLE KEYS")
        
        for file in os.listdir(Config.DATA_DIR):
            if not (file.endswith('.csv') and any(qt in file for qt in ['1T', '2T', '3T', '4T'])):
                continue
                
            file_path = os.path.join(Config.DATA_DIR, file)
            
            if not os.path.exists(file_path):
                logger.error(f"Arquivo {file_path} não encontrado!")
                continue
                
            try:
                df = pd.read_csv(file_path, sep=';', dtype=str)
                
                df = df.map(sanitize_data)  
                
                df['VL_SALDO_INICIAL'] = pd.to_numeric(
                    df['VL_SALDO_INICIAL'].str.replace(r'[.,]', '', regex=True),
                    errors='coerce'
                ) / 100
                
                df['VL_SALDO_FINAL'] = pd.to_numeric(
                    df['VL_SALDO_FINAL'].str.replace(r'[.,]', '', regex=True),
                    errors='coerce'
                ) / 100
                
                quarter = file[0]
                year = file[2:6]
                df['QUARTER'] = quarter
                df['ANO'] = year
                
                temp_filename = f"temp_{os.path.basename(file)}".replace(' ', '_')
                temp_file = os.path.join(Config.DATA_DIR, temp_filename)
                
                # Salvar com encoding adequado
                df.to_csv(temp_file, index=False, sep=';', encoding='utf-8')
                
                # Converter caminho para formato MySQL
                mysql_path = temp_file.replace('\\', '/')
                
                # Executar com tratamento de erros
                cursor.execute(f"""
                    LOAD DATA LOCAL INFILE '{mysql_path}'
                    INTO TABLE despesas
                    FIELDS TERMINATED BY ';'
                    OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n'
                    IGNORE 1 ROWS
                    (DATA, REG_ANS, CD_CONTA_CONTABIL, DESCRICAO,
                     VL_SALDO_INICIAL, VL_SALDO_FINAL, QUARTER, ANO)
                """)
                
                # Verificar se o arquivo temporário foi criado
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                else:
                    logger.warning(f"Arquivo temporário não encontrado: {temp_file}")
                    
            except Exception as e:
                logger.error(f"Erro ao processar {file}: {str(e)}")
                continue
                
        cursor.execute("ALTER TABLE despesas ENABLE KEYS")
        conn.commit()
        logger.info("Dados de despesas importados com sucesso")
        
    except Exception as e:
        logger.error(f"Erro geral ao processar despesas: {str(e)}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
            
def consultas(conn: mysql.connector.MySQLConnection) -> None:
    """Realiza as consultas no banco de dados"""
    
    try:
        cursor = conn.cursor()
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
        
    except Exception as e:
        logger.error(f"Erro na consulta SQL: {str(e)}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()

def main():
    """Fluxo principal"""
    
    try:
        setup_directories()
        
        # Download de dados
        logger.info("Iniciando download de dados...")
        download_and_extract(Config.URLS['operadoras'], 'Relatorio_cadop.csv')
        
        for year in [2023, 2024]:
            for quarter in ['1T', '2T', '3T', '4T']:
                url = f"{Config.URLS['despesas_base']}{year}/{quarter}{year}.zip"
                download_and_extract(url, f"{quarter}{year}.zip")
        
        # Conexão com o banco
        conn = mysql.connector.connect(**Config.DB_CONFIG)
        
        # Processamento de dados
        create_tables(conn)
        process_operadoras(conn)
        process_despesas(conn)
        # Consultas
        consultas(conn)
        
    except Exception as e:
        logger.error(f"Erro na execução: {str(e)}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            logger.info("Conexão com o banco encerrada")

if __name__ == "__main__":
    main()