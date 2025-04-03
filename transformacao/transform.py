import pdfplumber
import pandas as pd
import zipfile
import os

def setup_directories() -> tuple:
    """Configura os diretórios e retorna os caminhos necessários"""
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    pdf_path = os.path.join(base_dir, "pdf_anexos", "Anexo_I_Rol_2021RN_465.2021_RN627L.2024.pdf")
    output_dir = os.path.join(base_dir, "transform_data")
    
    os.makedirs(output_dir, exist_ok=True)
    
    csv_path = os.path.join(output_dir, "tabela_extraida.csv")
    zip_path = os.path.join(output_dir, "tabela_extraida.zip")
    
    return pdf_path, csv_path, zip_path

def extract_table_from_pdf(pdf_path: str) -> pd.DataFrame:
    """Extrai tabelas de um arquivo PDF e retorna um DataFrame"""
    
    all_data = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                all_data.extend(table)
    
    df = pd.DataFrame(all_data)
    df = df.replace("OD", "Seg. Odontologica")
    df = df.replace("AMB", "Seg. Ambulatorial")
    return df

def save_to_csv(df: pd.DataFrame, csv_path: str) -> None:
    """Salva o DataFrame em um arquivo CSV"""
    
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"Tabela extraída e salva em {csv_path}")

def create_zip_file(csv_path: str, zip_path: str) -> None:
    """Cria um arquivo ZIP contendo o CSV"""
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path, os.path.basename(csv_path))
    print(f"Arquivo compactado salvo em {zip_path}")

def main():
    """Fluxo principal"""
    
    pdf_path, csv_path, zip_path = setup_directories()
    df = extract_table_from_pdf(pdf_path)
    save_to_csv(df, csv_path)
    create_zip_file(csv_path, zip_path)

if __name__ == "__main__":
    main()