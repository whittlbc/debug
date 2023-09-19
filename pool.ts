import { Pool } from 'https://deno.land/x/postgres@v0.14.0/mod.ts'

const user = 'benwhittle'
const url = `postgres://${user}:@localhost:5432/live-object-testing?sslmode=disable`
const pool = new Pool(url, 10, true)

const random = (): string => (Math.random() * 1000000).toFixed(0)

async function getPoolConnection() {
    let conn
    try {
        conn = await pool.connect()
    } catch (err) {
        conn && conn.release()
        throw `Getting pool connection failed: ${JSON.stringify(err)}`
    }
    return conn
}

async function doesSchemaExist(name: string): Promise<boolean> {
    const query = {
        sql: `select count(*) from information_schema.schemata where schema_name in ($1, $2)`,
        bindings: [name, `"${name}"`],
    }
    let rows = []
    try {
        rows = await performQuery(query)
    } catch (err) {
        throw `Error checking if schema exists (${name}): ${JSON.stringify(err)}`
    }
    const count = rows ? Number((rows[0] || {}).count || 0) : 0
    return count > 0
}

async function performQuery(query: any, silent: boolean = true): Promise<any[]> {
    const { sql, bindings } = query
    const conn = await getPoolConnection()
    const tx = conn.createTransaction(`query_${random()}`)

    let result
    try {
        await tx.begin()
        silent || console.info(sql, bindings)
        result = await tx.queryArray({ text: sql, args: bindings })
        await tx.commit()
    } catch (err) {
        throw `Query failed: ${err.message}`
    } finally {
        conn.release()
    }
    if (!result) {
        throw 'Empty query result'
    }

    return mapColumnNamesToPgResult(result)
}

function mapColumnNamesToPgResult(result): any[] {
    return (result.rows || []).map((values) => {
        const record = {}
        result.rowDescription.columns.forEach((col, i) => {
            let value = values[i]
            if (typeof value === 'bigint') {
                value = value.toString()
            }
            record[col.name] = value
        })
        return record
    })
}

const exists = await doesSchemaExist('allov2')
console.log('EXISTS:', exists)