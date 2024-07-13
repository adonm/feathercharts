<template>
    <q-layout view="hHh lpR fFf">
        <q-header elevated class="bg-primary text-white">
            <q-toolbar>
                <q-toolbar-title>DuckDB API Tester</q-toolbar-title>
            </q-toolbar>
        </q-header>

        <q-page-container>
            <q-page padding>
                <q-card flat bordered>
                    <q-card-section>
                        <q-input v-model="sql" type="textarea" label="SQL Query/Statement" rows="4" />
                    </q-card-section>

                    <q-card-section>
                        <q-btn-group spread>
                            <q-btn color="primary" label="Execute Query" @click="executeQuery" />
                            <q-btn color="secondary" label="Execute Statement" @click="executeStatement" />
                            <q-btn color="accent" label="Load TPC-H Data" @click="loadTPCHData" />
                            <q-btn color="positive" label="Export CSV" @click="exportCSV" />
                            <q-btn color="negative" label="Export JSON" @click="exportJSON" />
                        </q-btn-group>
                    </q-card-section>

                    <q-card-section v-if="error">
                        <q-banner rounded class="bg-red-1 text-red">
                            {{ error }}
                        </q-banner>
                    </q-card-section>

                    <q-card-section v-if="message">
                        <q-banner rounded class="bg-green-1 text-green">
                            {{ message }}
                        </q-banner>
                    </q-card-section>

                    <q-card-section v-if="result">
                        <q-table title="Query Result" :rows="result" :columns="columns" row-key="index" dense />
                    </q-card-section>
                </q-card>
            </q-page>
        </q-page-container>
    </q-layout>
</template>

<script>
import { ref, computed } from 'vue'

export default {
    setup() {
        const sql = ref('')
        const error = ref('')
        const message = ref('')
        const result = ref(null)

        const columns = computed(() => {
            if (!result.value || result.value.length === 0) return []
            return Object.keys(result.value[0]).map(key => ({
                name: key,
                label: key.charAt(0).toUpperCase() + key.slice(1),
                field: key,
                align: 'left'
            }))
        })

        async function executeQuery() {
            try {
                const response = await fetch('/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sql: sql.value })
                })
                if (!response.ok) throw new Error('Network response was not ok')
                result.value = await response.json()
                message.value = 'Query executed successfully'
                error.value = ''
            } catch (err) {
                error.value = 'Error executing query: ' + err.message
                message.value = ''
                result.value = null
            }
        }

        async function executeStatement() {
            try {
                const response = await fetch('/execute', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sql: sql.value })
                })
                if (!response.ok) throw new Error('Network response was not ok')
                const data = await response.json()
                message.value = data.message || 'Statement executed successfully'
                error.value = ''
                result.value = null
            } catch (err) {
                error.value = 'Error executing statement: ' + err.message
                message.value = ''
                result.value = null
            }
        }

        async function loadTPCHData() {
            sql.value = 'CALL dbgen(sf = 0.01);'
            message.value = 'TPC-H data generation command prepared. Click "Execute Statement" to load.'
            error.value = ''
        }

        async function exportCSV() {
            if (!result.value) {
                error.value = 'No data to export. Run a query first.'
                return
            }
            const csv = [
                columns.value.map(col => col.label).join(','),
                ...result.value.map(row => columns.value.map(col => row[col.field]).join(','))
            ].join('\n')

            const blob = new Blob([csv], { type: 'text/csv' })
            const link = document.createElement('a')
            link.href = URL.createObjectURL(blob)
            link.download = 'export.csv'
            link.click()
            message.value = 'CSV exported successfully'
        }

        function exportJSON() {
            if (!result.value) {
                error.value = 'No data to export. Run a query first.'
                return
            }
            const json = JSON.stringify(result.value, null, 2)
            const blob = new Blob([json], { type: 'application/json' })
            const link = document.createElement('a')
            link.href = URL.createObjectURL(blob)
            link.download = 'export.json'
            link.click()
            message.value = 'JSON exported successfully'
        }

        return {
            sql,
            error,
            message,
            result,
            columns,
            executeQuery,
            executeStatement,
            loadTPCHData,
            exportCSV,
            exportJSON
        }
    }
}
</script>