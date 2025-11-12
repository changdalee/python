
import sqlite3

# 创建磁盘数据库（自动生成文件）
def create_disk_db():
    conn = sqlite3.connect('akshare.db')  # 相对路径
    # conn = sqlite3.connect('/path/to/your_database.db')  # 绝对路径
    conn.close()
    print("磁盘数据库创建成功")

# 创建内存数据库（临时使用）
def create_memory_db():
    conn = sqlite3.connect(':memory:')
    conn.close()
    print("内存数据库创建成功")

if __name__ == '__main__':
    create_disk_db()
    create_memory_db()
