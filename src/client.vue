<!-- src/client.vue -->
<template>
  <q-layout view="hHh lpR fFf">
    <q-header elevated class="bg-primary text-white">
      <q-toolbar>
        <q-toolbar-title>DuckDB API Tester</q-toolbar-title>
      </q-toolbar>
    </q-header>

    <q-page-container>
      <q-page padding>
        <q-card>
          <q-card-section>
            <q-input
              v-model="sql"
              type="textarea"
              label="SQL Query"
              outlined
            />
          </q-card-section>

          <q-card-actions align="right">
            <q-btn label="Execute Query" color="primary" @click="executeQuery" />
            <q-btn label="Execute Statement" color="secondary" @click="executeStatement" />
            <q-btn label="Load TPC-H Data" color="accent" @click="loadTpchData" />
            <q-btn label="Export CSV" color="positive" @click="exportCsv" />
            <q-btn label="Export JSON" color="negative" @click="exportJson" />
          </q-card-actions>
        </q-card>

        <q-card v-if="result" class="q-mt-md">
          <q-card-section>
            <pre>{{ JSON.stringify(result, null, 2) }}</pre>
          </q-card-section>
        </q-card>

        <q-dialog v-model="errorDialog">
          <q-card>
            <q-card-section>
              <div class="text-h6">Error</div>
            </q-card-section>

            <q-card-section class="q-pt-none">
              {{ error }}
            </q-card-section>

            <q-card-actions align="right">
              <q-btn flat label="OK" color="primary" v-close-popup />
            </q-card-actions>
          </q-card>
        </q-dialog>
      </q-page>
    </q-page-container>
  </q-layout>
</template>

<script>
import { ref } from 'vue'
import { useQuasar } from 'quasar'

export default {
  setup() {
    const $q = useQuasar()
    const sql = ref('')
    const result = ref(null)
    const error = ref('')
    const errorDialog = ref(false)

    async function executeQuery() {
      try {
        const response = await fetch('/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql: sql.value })
        })
        result.value = await response.json()
      } catch (err) {
        error.value = err.message
        errorDialog.value = true
      }
    }

    async function executeStatement() {
      try {
        const response = await fetch('/execute', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql: sql.value })
        })
        result.value = await response.json()
      } catch (err) {
        error.value = err.message
        errorDialog.value = true
      }
    }

    function loadTpchData() {
      $q.notify('TPC-H data loading not implemented')
    }

    function exportCsv() {
      $q.notify('CSV export not implemented')
    }

    function exportJson() {
      $q.notify('JSON export not implemented')
    }

    return {
      sql,
      result,
      error,
      errorDialog,
      executeQuery,
      executeStatement,
      loadTpchData,
      exportCsv,
      exportJson
    }
  }
}
</script>