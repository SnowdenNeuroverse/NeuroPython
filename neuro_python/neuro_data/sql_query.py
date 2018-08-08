"""
Parameterised sql query that can be executed on a sql table in Neuroverse
"""
"""
SelectClause::String
FromTableName::Union{String,Void}
FromSubQuery::Union{AbstractSqlQuery,Void}
FromAlias::Union{String,Void}
Joins::Union{Array{AbstractSqlJoin,1},Void}
WhereClause::Union{String,Void}
GroupByClause::Union{String,Void}
HavingClause::Union{String,Void}
OrderByClause::Union{String,Void}
"""
def sql_query(select: str = None, table_name: str = None, sub_query: "sql_query" = None,
              alias: str = None, joins: "List[sql_join]" = None, where: str = None,
              group_by: str = None, having: str = None, order_by: str = None):
    """
    Returns a sql query object
    """
    if select is None:
        raise Exception("select must be supplied")
    if not(table_name is not None and sub_query is None) and not(table_name is None and sub_query is not None):
        raise Exception("table_name or sub_query must be supplied")
    if joins is not None and alias is None:
        raise Exception("An alias must be used when joins are supplied")
    return {"SelectClause" : select, "FromTableName" : table_name, "FromSubQuery" : sub_query,
            "FromAlias" : alias, "Joins" : joins, "WhereClause" : where, "GroupByClause" : group_by,
            "HavingClause" : having, "OrderByClause" : order_by}

def sql_join(join_type: str = None, table_name: str = None, sub_query: "sql_query" = None,
             alias: str = None, clause: str = None):
    """
    Returns a sql join object
    """
    if join_type is None:
        raise Exception("Join type must be supplied")
    if alias is None:
        raise Exception("Alias must be supplied")
    if clause is None:
        raise Exception("Clause must be supplied")
    if not(table_name is not None and sub_query is None) and not(table_name is None and sub_query is not None):
        raise Exception("table_name or sub_query must be supplied")
    return {"JoinType" : join_type, "JoinTableName" : table_name, "JoinSubQuery" : sub_query,
            "JoinAlias" : alias, "JoinClause" : clause}
