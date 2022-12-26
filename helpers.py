from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import table, column

def dict_to_list(dictionary):
    if not isinstance(dictionary, dict):
        return dictionary
    else:
        return list(dictionary.values())

def df_to_sql(df, sql_table, con, returning_col=None):
    if not df.empty:
        columns = [column(i) for i in df.columns]
        my_table = table(sql_table, *columns)
        insert_stmt = insert(my_table).values(list(df.itertuples(name=None, index=False)))
        if returning_col:
            insert_stmt = insert_stmt.returning(my_table.c[returning_col])
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing()
        res = con.execute(do_nothing_stmt)
        if returning_col:
            return res.fetchall()
    else:
        return []


