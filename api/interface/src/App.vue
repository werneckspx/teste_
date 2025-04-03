<template>
  <div class="container">
    <h1>Busca de Operadoras ANS</h1>
    <div class="search-box">
      <input 
        type="text" 
        v-model="searchQuery" 
        @input="performSearch" 
        placeholder="Digite nome, cidade ou CNPJ..."
      >
    </div>

    <div v-if="loading" class="loading">Carregando...</div>

    <div v-if="results.length" class="results">
      <div v-for="item in results" :key="item.Registro_ANS" class="card">
        <h3>{{ item.Razao_Social }}</h3>
        <div class="details">
          <p><strong>CNPJ:</strong> {{ formatCNPJ(item.CNPJ) }}</p>
          <p><strong>Cidade:</strong> {{ item.Cidade }}/{{ item.UF }}</p>
          <p><strong>Registro ANS:</strong> {{ item.Registro_ANS }}</p>
        </div>
      </div>
    </div>

    <div v-else-if="!loading" class="no-results">
      Nenhum resultado encontrado para "{{ searchQuery }}"
    </div>
  </div>
</template>

<script>
import axios from 'axios';

export default {
  data() {
    return {
      searchQuery: '',
      results: [],
      loading: false
    }
  },
  methods: {
    async performSearch() {
      if (this.searchQuery.length > 2) {
        this.loading = true;
        try {
          const response = await axios.get('http://localhost:5000/search', {
            params: { q: this.searchQuery }
          });
          this.results = response.data;
        } catch (error) {
          console.error('Erro na busca:', error);
        }
        this.loading = false;
      }
    },
    formatCNPJ(cnpj) {
      return cnpj.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5");
    }
  }
}
</script>

<style>
.container {
  max-width: 800px;
  margin: 0 auto;
  padding: 20px;
}

.search-box {
  margin: 20px 0;
}

input {
  width: 100%;
  padding: 12px;
  font-size: 16px;
  border: 2px solid #ccc;
  border-radius: 4px;
}

.card {
  background: #fff;
  border: 1px solid #ddd;
  border-radius: 8px;
  padding: 15px;
  margin: 10px 0;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.loading {
  text-align: center;
  padding: 20px;
  color: #666;
}

.no-results {
  text-align: center;
  padding: 20px;
  color: #666;
}

.details p {
  margin: 5px 0;
  color: #555;
}
</style>