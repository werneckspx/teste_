from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error

#Cria uma instancia
app = Flask(__name__)
CORS(app)

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="270211Tfj",
            database="dados"
        )
        return conn
    except Error as e:
        print("Erro de conexão:", e)
        return None

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q', '')
    
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
            
        cursor = conn.cursor(dictionary=True)  # Usar dictionary=True para resultados como dicionários
        
        # Busca case-insensitive com COLLATE
        search_term = f"%{query}%"
        cursor.execute("""
            SELECT * FROM operadoras 
            WHERE CNPJ LIKE %s COLLATE utf8mb4_general_ci
            LIMIT 10
        """, (search_term,))
        
        results = cursor.fetchall()
        
        # Fechar cursor e conexão
        cursor.close()
        conn.close()
        
        return jsonify(results)
        
    except Error as e:
        print("Erro na consulta:", e)
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        print("Erro geral:", e)
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True)