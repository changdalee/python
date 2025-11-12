import pandas as pd
import sqlite3
from sqlite3 import OperationalError


def df_to_sqlite(df, table_name, db_name='data.db', if_exists='replace', index=False):
    """
    将pandas DataFrame存储到SQLite3数据库

    参数:
        df: 要存储的DataFrame
        table_name: 要创建的表名
        db_name: SQLite数据库文件名，默认为'data.db'
        if_exists: 表存在时的处理方式，可选'replace'、'append'、'fail'
        index: 是否将DataFrame的索引作为一列存储
    """
    try:
        # 连接到SQLite数据库（如果不存在则创建）
        conn = sqlite3.connect(db_name)

        # 将DataFrame写入SQLite表
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,
            index=index
        )

        # 提交事务并关闭连接
        conn.commit()
        conn.close()

        print(f"成功将DataFrame存储到SQLite表 '{table_name}'，共 {len(df)} 行数据")
        return True

    except OperationalError as e:
        print(f"数据库操作错误: {str(e)}")
        return False
    except Exception as e:
        print(f"发生错误: {str(e)}")
        return False


# 示例用法
if __name__ == "__main__":
    # 创建示例DataFrame
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['张三', '李四', '王五', '赵六', '钱七'],
        'email': ['zhangsan@example.com', 'lisi@example.com',
                  'wangwu@example.com', 'zhaoliu@example.com', 'qianqi@example.com'],
        'registration_date': ['2023-01-15', '2023-02-20', '2023-03-05',
                              '2023-04-10', '2023-05-18']
    }
    df = pd.DataFrame(data)

    # 存储到SQLite数据库
    df_to_sqlite(
        df=df,
        table_name='users',
        db_name='user_database.db',
        if_exists='replace'
    )
