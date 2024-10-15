const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');

class KVDatabase {
    constructor(dir) {
        this.dbPath = path.join(dir, 'my-database.db');
        this.db = null;

        // Create directory if it does not exist
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        // Open the database
        this.open();
    }

    open() {
        this.db = new sqlite3.Database(this.dbPath, (err) => {
            if (err) {
                console.error("Error opening database: " + err.message);
                this.db = null;
            } else {
                this.db.run("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);");
            }
        });
    }

    // Write data
    set(name, data) {
        return new Promise((resolve, reject) => {
            const value = JSON.stringify(data);
            const stmt = this.db.prepare("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?);");
            stmt.run(name, value, function (err) {
                if (err) {
                    reject(false);
                } else {
                    resolve(true);
                }
            });
            stmt.finalize();
        });
    }

    // Read data
    get(name) {
        return new Promise((resolve, reject) => {
            this.db.get("SELECT value FROM kv WHERE key = ?", [name], (err, row) => {
                if (err) {
                    reject(null);
                } else {
                    resolve(row ? JSON.parse(row.value) : null);
                }
            });
        });
    }

    // Delete data
    delete(name) {
        return new Promise((resolve, reject) => {
            this.db.run("DELETE FROM kv WHERE key = ?", [name], function (err) {
                if (err) {
                    reject(false);
                } else {
                    resolve(this.changes > 0);
                }
            });
        });
    }

    // Get all keys
    listKey() {
        return new Promise((resolve, reject) => {
            this.db.all("SELECT key FROM kv", [], (err, rows) => {
                if (err) {
                    reject([]);
                } else {
                    resolve(rows.map(row => row.key));
                }
            });
        });
    }

    // Close the database
    close() {
        return new Promise((resolve, reject) => {
            if (this.db) {
                this.db.close((err) => {
                    if (err) {
                        reject(false);
                    } else {
                        this.db = null;
                        resolve(true);
                    }
                });
            } else {
                resolve(true);
            }
        });
    }
}

// Example usage
(async () => {
    const db = new KVDatabase('./data');

    await db.set('name', 'John Doe');
    console.log(await db.get('name')); // 'John Doe'

    await db.set('age', 30);
    console.log(await db.get('age')); // 30

    console.log(await db.listKey()); // ['name', 'age']
    console.log(await db.get('name')); // null

    await db.close();
})();
