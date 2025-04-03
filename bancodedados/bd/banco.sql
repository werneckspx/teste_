CREATE DATABASE IF NOT EXISTS dados;

USE dados;

-- Tabela para operadoras (dados do arquivo principal)
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

-- Tabela para despesas (dados dos 8 arquivos)
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

DELETE FROM operadoras;
SELECT * FROM operadoras;
SELECT * FROM despesas;
SHOW WARNINGS;

SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

GRANT FILE ON *.* TO 'root'@'localhost';
FLUSH PRIVILEGES;

SELECT 
    o.Razao_Social, 
    SUM(d.VL_SALDO_FINAL - d.VL_SALDO_INICIAL) AS Total_Despesas
FROM despesas d
JOIN operadoras o ON d.REG_ANS = o.Registro_ANS
WHERE 
    d.DESCRICAO LIKE '%EVENTOS SINISTROS CONHECIDOS OU AVISADOS  DE ASSISTENCIA A SAUDE MEDICO HOSPITALAR%'  -- Texto sanitizado
    AND d.ANO = 2024  -- Ano fixo para teste
    AND d.QUARTER = 4  -- Trimestre fixo para teste
GROUP BY o.Razao_Social
ORDER BY Total_Despesas DESC
LIMIT 10;

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