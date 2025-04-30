import pytest
from odibi_de_v2.transformer import SQLTransformerFromConfig

def test_basic_select_query():
    config = {
        "query_type": "select",
        "table": "employees",
        "columns": ["id", "name", "salary"],
        "where": ["salary > 50000"],
        "order_by": [{"column": "salary", "order": "DESC"}],
        "limit": 10
    }

    transformer = SQLTransformerFromConfig(config=config)
    query = transformer.transform()

    assert "SELECT" in query
    assert "FROM" in query
    assert "WHERE" in query
    assert "ORDER BY" in query
    assert "LIMIT 10" in query

def test_case_when_query():
    config = {
        "query_type": "select",
        "table": "orders",
        "columns": ["id"],
        "case_when": [
            {
                "cases": [
                    {"condition": "amount > 100", "result": "'High'"},
                    {"condition": "amount <= 100", "result": "'Low'"}
                ],
                "else_value": "'Unknown'",
                "alias": "order_value_category"
            }
        ]
    }

    transformer = SQLTransformerFromConfig(config=config)
    query = transformer.transform()

    assert "CASE" in query
    assert "WHEN" in query
    assert "THEN" in query
    assert "ELSE" in query
    assert "END" in query

def test_cte_and_union_query():
    config = {
        "query_type": "select",
        "ctes": [
            {
                "name": "high_salary_employees",
                "query": {
                    "query_type": "select",
                    "table": "employees",
                    "columns": ["id", "name", "salary"],
                    "where": ["salary > 100000"]
                }
            }
        ],
        "table": "high_salary_employees",
        "columns": ["id", "name", "salary"],
        "unions": [
            {
                "query": {
                    "query_type": "select",
                    "table": "contractors",
                    "columns": ["id", "name", "salary"],
                    "where": ["salary > 100000"]
                },
                "all": True
            }
        ]
    }

    transformer = SQLTransformerFromConfig(config=config)
    query = transformer.transform()

    assert "WITH" in query
    assert "UNION ALL" in query
    assert "FROM" in query
