#!/usr/bin/env node

/**
 * Script de Sincronização SonarQube CSV → MySQL (VERSÃO CORRIGIDA)
 * Executa a cada minuto via cron para manter o banco atualizado
 * 
 * Uso: node sync-sonar-data.js
 * Cron: * * * * * /usr/bin/node /path/to/sync-sonar-data.js >> /var/log/sonar-sync.log 2>&1
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http'); // Adicionado para suporte a redirecionamentos
const csv = require('csv-parser');
const mysql = require('mysql2/promise');
require('dotenv').config();

// Configurações
const CONFIG = {
  // URL do CSV no Google Drive (substitua pelo seu)
  csvUrl: process.env.CSV_URL || 'https://drive.google.com/uc?id=1pP4S5Q3erfPwc8hmf-8xmgrpbK8CD7vy&export=download',
  
  // Configuração MySQL
  mysql: {
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'devops_metrics'
  },
  
  // Arquivo para controle de sincronização
  lastSyncFile: path.join(__dirname, '.last-sync.json'),
  csvCacheFile: path.join(__dirname, 'sonar-data-cache.csv'),
  
  // Log
  enableLog: process.env.ENABLE_LOG !== 'false',
  logFile: process.env.LOG_FILE || '/tmp/sonar-sync.log',
  
  // Configurações para download
  maxRedirects: 5,
  timeout: 30000
};

class SonarSyncManager {
  constructor() {
    this.connection = null;
    this.lastSync = this.loadLastSync();
  }

  /**
   * Função principal de sincronização
   */
  async sync() {
    try {
      this.log('🔄 Iniciando sincronização...');
      
      // 1. Baixar CSV do Google Drive
      const csvPath = await this.downloadCSV();
      
      // 2. Verificar se há mudanças
      const hasChanges = await this.checkForChanges(csvPath);
      
      if (!hasChanges) {
        this.log('✅ Nenhuma alteração detectada. Sincronização não necessária.');
        return;
      }
      
      // 3. Conectar ao MySQL
      await this.connectMySQL();
      
      // 4. Processar CSV e inserir dados
      const insertedCount = await this.processCSVAndInsert(csvPath);
      
      // 5. Atualizar registro de última sincronização
      this.updateLastSync(csvPath);
      
      this.log(`✅ Sincronização concluída. ${insertedCount} registros inseridos/atualizados.`);
      
    } catch (error) {
      this.log(`❌ Erro na sincronização: ${error.message}`, true);
      throw error;
    } finally {
      if (this.connection) {
        await this.connection.end();
      }
    }
  }

  /**
   * Download do CSV do Google Drive com suporte a redirecionamentos
   */
  async downloadCSV() {
    return new Promise((resolve, reject) => {
      this.log('📥 Baixando CSV do Google Drive...');
      
      this.downloadWithRedirects(CONFIG.csvUrl, CONFIG.csvCacheFile, 0)
        .then(() => {
          this.log('✅ CSV baixado com sucesso');
          resolve(CONFIG.csvCacheFile);
        })
        .catch(reject);
    });
  }

  /**
   * Download com suporte a redirecionamentos automáticos
   */
  async downloadWithRedirects(url, filePath, redirectCount = 0) {
    return new Promise((resolve, reject) => {
      if (redirectCount > CONFIG.maxRedirects) {
        reject(new Error(`Muitos redirecionamentos (${redirectCount})`));
        return;
      }

      const file = fs.createWriteStream(filePath);
      const urlObj = new URL(url);
      const client = urlObj.protocol === 'https:' ? https : http;

      const request = client.get(url, (response) => {
        // Lidar com redirecionamentos
        if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
          file.close();
          fs.unlink(filePath, () => {}); // Limpar arquivo
          
          const redirectUrl = response.headers.location;
          this.log(`🔀 Redirecionamento ${redirectCount + 1}: ${redirectUrl}`);
          
          this.downloadWithRedirects(redirectUrl, filePath, redirectCount + 1)
            .then(resolve)
            .catch(reject);
          return;
        }

        // Verificar se a resposta é bem-sucedida
        if (response.statusCode !== 200) {
          file.close();
          fs.unlink(filePath, () => {});
          reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
          return;
        }

        // Pipe do response para o arquivo
        response.pipe(file);

        file.on('finish', () => {
          file.close();
          resolve();
        });

        file.on('error', (error) => {
          fs.unlink(filePath, () => {});
          reject(error);
        });

      });

      request.on('error', (error) => {
        file.close();
        fs.unlink(filePath, () => {});
        reject(error);
      });

      request.setTimeout(CONFIG.timeout, () => {
        request.destroy();
        file.close();
        fs.unlink(filePath, () => {});
        reject(new Error('Timeout no download'));
      });
    });
  }

  /**
   * Verifica se o CSV mudou desde a última sincronização
   */
  async checkForChanges(csvPath) {
    try {
      const stats = fs.statSync(csvPath);
      const currentHash = await this.getFileHash(csvPath);
      
      const hasChanges = (
        !this.lastSync.fileHash || 
        this.lastSync.fileHash !== currentHash ||
        !this.lastSync.lastModified ||
        stats.mtime.getTime() !== this.lastSync.lastModified
      );
      
      if (hasChanges) {
        this.log(`🔍 Mudanças detectadas no CSV (hash: ${currentHash})`);
      }
      
      return hasChanges;
      
    } catch (error) {
      this.log(`⚠️ Erro ao verificar mudanças: ${error.message}`);
      return true; // Em caso de erro, assumir que há mudanças
    }
  }

  /**
   * Gera hash do arquivo para detecção de mudanças
   */
  async getFileHash(filePath) {
    const crypto = require('crypto');
    const content = fs.readFileSync(filePath);
    return crypto.createHash('md5').update(content).digest('hex');
  }

  /**
   * Conecta ao MySQL
   */
  async connectMySQL() {
    try {
      this.log('🔌 Conectando ao MySQL...');
      this.connection = await mysql.createConnection(CONFIG.mysql);
      await this.connection.execute('SELECT 1'); // Teste de conexão
      this.log('✅ Conectado ao MySQL com sucesso');
    } catch (error) {
      throw new Error(`Falha ao conectar MySQL: ${error.message}`);
    }
  }

  /**
   * Processa o CSV e insere dados no MySQL
   */
  async processCSVAndInsert(csvPath) {
    return new Promise((resolve, reject) => {
      const records = [];
      let insertedCount = 0;
      
      // Verificar se o arquivo CSV existe e não está vazio
      const stats = fs.statSync(csvPath);
      if (stats.size === 0) {
        this.log('⚠️ Arquivo CSV está vazio');
        resolve(0);
        return;
      }

      fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
          // Processar cada linha do CSV
          const record = this.parseCSVRow(row);
          if (record) {
            records.push(record);
          }
        })
        .on('end', async () => {
          try {
            this.log(`📊 Processando ${records.length} registros...`);
            
            for (const record of records) {
              const inserted = await this.insertOrUpdateRecord(record);
              if (inserted) insertedCount++;
            }
            
            resolve(insertedCount);
            
          } catch (error) {
            reject(error);
          }
        })
        .on('error', reject);
    });
  }

  /**
   * Converte linha CSV para objeto de registro
   */
  parseCSVRow(row) {
    try {
      return {
        timestamp: row.timestamp ? new Date(row.timestamp) : null,
        server_url: row.server_url || null,
        task_id: row.task_id || null,
        status: row.status || null,
        analysed_at: row.analysed_at ? new Date(row.analysed_at) : null,
        revision: row.revision || null,
        project_key: row.project_key || null,
        project_name: row.project_name || null,
        project_url: row.project_url || null,
        branch_name: row.branch_name || null,
        branch_type: row.branch_type || null,
        branch_is_main: row.branch_is_main === 'true' || row.branch_is_main === '1',
        quality_gate_name: row.quality_gate_name || null,
        quality_gate_status: row.quality_gate_status || null,
        new_reliability_rating: this.parseFloat(row.new_reliability_rating),
        new_reliability_rating_status: row.new_reliability_rating_status || null,
        new_security_rating: this.parseFloat(row.new_security_rating),
        new_security_rating_status: row.new_security_rating_status || null,
        new_maintainability_rating: this.parseFloat(row.new_maintainability_rating),
        new_maintainability_rating_status: row.new_maintainability_rating_status || null,
        new_coverage: this.parseFloat(row.new_coverage),
        new_coverage_status: row.new_coverage_status || null,
        new_coverage_threshold: this.parseFloat(row.new_coverage_threshold),
        new_duplicated_lines_density: this.parseFloat(row.new_duplicated_lines_density),
        new_duplicated_lines_density_status: row.new_duplicated_lines_density_status || null,
        new_duplicated_lines_density_threshold: this.parseFloat(row.new_duplicated_lines_density_threshold),
        new_security_hotspots_reviewed: this.parseFloat(row.new_security_hotspots_reviewed),
        new_security_hotspots_reviewed_status: row.new_security_hotspots_reviewed_status || null,
        new_security_hotspots_reviewed_threshold: this.parseFloat(row.new_security_hotspots_reviewed_threshold)
      };
    } catch (error) {
      this.log(`⚠️ Erro ao processar linha CSV: ${error.message}`);
      return null;
    }
  }

  /**
   * Helper para converter string para float
   */
  parseFloat(value) {
    if (!value || value === '') return null;
    const parsed = parseFloat(value);
    return isNaN(parsed) ? null : parsed;
  }

  /**
   * Insere ou atualiza registro no MySQL (UPSERT)
   */
  async insertOrUpdateRecord(record) {
    try {
      // Verificar se já existe usando task_id + project_key
      const [existing] = await this.connection.execute(
        'SELECT id FROM sonarqube_analysis WHERE task_id = ? AND project_key = ? LIMIT 1',
        [record.task_id, record.project_key]
      );

      if (existing.length > 0) {
        // Registro já existe - skip ou update conforme necessário
        return false;
      }

      // Inserir novo registro
      const query = `
        INSERT INTO sonarqube_analysis (
          timestamp, server_url, task_id, status, analysed_at, revision,
          project_key, project_name, project_url, branch_name, branch_type, branch_is_main,
          quality_gate_name, quality_gate_status,
          new_reliability_rating, new_reliability_rating_status,
          new_security_rating, new_security_rating_status,
          new_maintainability_rating, new_maintainability_rating_status,
          new_coverage, new_coverage_status, new_coverage_threshold,
          new_duplicated_lines_density, new_duplicated_lines_density_status, new_duplicated_lines_density_threshold,
          new_security_hotspots_reviewed, new_security_hotspots_reviewed_status, new_security_hotspots_reviewed_threshold
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;

      const values = [
        record.timestamp, record.server_url, record.task_id, record.status, record.analysed_at, record.revision,
        record.project_key, record.project_name, record.project_url, record.branch_name, record.branch_type, record.branch_is_main,
        record.quality_gate_name, record.quality_gate_status,
        record.new_reliability_rating, record.new_reliability_rating_status,
        record.new_security_rating, record.new_security_rating_status,
        record.new_maintainability_rating, record.new_maintainability_rating_status,
        record.new_coverage, record.new_coverage_status, record.new_coverage_threshold,
        record.new_duplicated_lines_density, record.new_duplicated_lines_density_status, record.new_duplicated_lines_density_threshold,
        record.new_security_hotspots_reviewed, record.new_security_hotspots_reviewed_status, record.new_security_hotspots_reviewed_threshold
      ];

      await this.connection.execute(query, values);
      return true;

    } catch (error) {
      this.log(`❌ Erro ao inserir registro: ${error.message}`, true);
      return false;
    }
  }

  /**
   * Carrega informações da última sincronização
   */
  loadLastSync() {
    try {
      if (fs.existsSync(CONFIG.lastSyncFile)) {
        return JSON.parse(fs.readFileSync(CONFIG.lastSyncFile, 'utf8'));
      }
    } catch (error) {
      this.log(`⚠️ Erro ao carregar última sync: ${error.message}`);
    }
    
    return {};
  }

  /**
   * Atualiza informações da última sincronização
   */
  updateLastSync(csvPath) {
    try {
      const stats = fs.statSync(csvPath);
      const fileHash = require('crypto')
        .createHash('md5')
        .update(fs.readFileSync(csvPath))
        .digest('hex');

      const syncInfo = {
        lastSync: new Date().toISOString(),
        lastModified: stats.mtime.getTime(),
        fileHash: fileHash,
        fileSize: stats.size
      };

      fs.writeFileSync(CONFIG.lastSyncFile, JSON.stringify(syncInfo, null, 2));
      this.log('✅ Informações de sincronização atualizadas');

    } catch (error) {
      this.log(`⚠️ Erro ao atualizar sync info: ${error.message}`);
    }
  }

  /**
   * Sistema de log
   */
  log(message, isError = false) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}`;
    
    if (CONFIG.enableLog) {
      console.log(logMessage);
      
      // Salvar em arquivo se especificado
      if (CONFIG.logFile) {
        try {
          fs.appendFileSync(CONFIG.logFile, logMessage + '\n');
        } catch (error) {
          // Silenciar erro de log para evitar loops
        }
      }
    }
    
    if (isError) {
      console.error(logMessage);
    }
  }

  /**
   * Método para testar apenas o download do CSV
   */
  async testDownload() {
    try {
      this.log('🧪 Testando download do CSV...');
      const csvPath = await this.downloadCSV();
      const stats = fs.statSync(csvPath);
      this.log(`✅ Download concluído! Arquivo: ${csvPath} (${stats.size} bytes)`);
      
      // Mostrar primeiras linhas do arquivo
      const content = fs.readFileSync(csvPath, 'utf8').split('\n').slice(0, 5).join('\n');
      this.log(`📄 Primeiras linhas:\n${content}`);
      
    } catch (error) {
      this.log(`❌ Erro no teste de download: ${error.message}`, true);
    }
  }
}

// Executar sincronização se chamado diretamente
if (require.main === module) {
  const syncManager = new SonarSyncManager();
  
  // Verificar se foi passado parâmetro para teste
  const args = process.argv.slice(2);
  
  if (args.includes('--test-download')) {
    // Modo teste - apenas baixar CSV
    syncManager.testDownload()
      .then(() => process.exit(0))
      .catch((error) => {
        console.error('❌ Falha no teste:', error.message);
        process.exit(1);
      });
  } else {
    // Modo normal - sincronização completa
    syncManager.sync()
      .then(() => {
        console.log('✅ Sincronização concluída com sucesso');
        process.exit(0);
      })
      .catch((error) => {
        console.error('❌ Falha na sincronização:', error.message);
        process.exit(1);
      });
  }
}

module.exports = SonarSyncManager;