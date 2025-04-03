import pdfplumber
import pandas as pd
import zipfile
import os

# Obtém o diretório base do projeto (onde o script está localizado)
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Define o caminho correto do PDF
pdf_path = os.path.join(base_dir, "pdf_anexos", "Anexo_I_Rol_2021RN_465.2021_RN627L.2024.pdf")

output_dir = os.path.join(base_dir, "transform_data")
csv_path = os.path.join(output_dir, "tabela_extraida.csv")
zip_path = os.path.join(output_dir, "tabela_extraida.zip")

# Garante que a pasta de saída existe
os.makedirs(output_dir, exist_ok=True)

def extract_table_from_pdf(pdf_path):
    all_data = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                all_data.extend(table)
    df = pd.DataFrame(all_data)
    # Substituições:
    df = df.replace("OD", "Seg. Odontologica")
    df = df.replace("AMB", "Seg. Ambulatorial")
    return df

df = extract_table_from_pdf(pdf_path)
df.to_csv(csv_path, index=False, encoding='utf-8-sig')
print(f"Tabela extraída e salva em {csv_path}")

with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    zipf.write(csv_path, os.path.basename(csv_path))  # Apenas o nome do arquivo dentro do ZIP

print(f"Arquivo compactado salvo em {zip_path}")
