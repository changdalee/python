import sqlite3

# 该代码创建了临时内存数据库，包含建表、插入数据和查询验证完整流程。内存数据库在程序结束后自动销毁，适合临时数据处理场景。
# 创建内存数据库连接


def create_in_memory_db():
    # 使用':memory:'作为特殊标识
    conn = sqlite3.connect(':memory:')

    # 创建示例表
    cursor = conn.cursor()
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS test_table(id INTEGER PRIMARY KEY,data TEXT NOT NULL)''')
    # 插入测试数据
    cursor.execute("INSERT INTO test_table (data) VALUES (?)",
                   ('内存数据库测试数据',))
    conn.commit()

    # 验证数据
    cursor.execute("SELECT * FROM test_table")
    print(cursor.fetchall())

    return conn


if __name__ == '__main__':
    db_conn = create_in_memory_db()
    db_conn.close()
