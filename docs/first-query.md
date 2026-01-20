# Your First Query

This tutorial walks through transpiling your first OpenCypher query to SQL.

---

## Step 1: Create a Schema

gsql2rsql needs a schema that maps your graph to SQL tables. Create `my_schema.json`:

```json
{
  "nodes": [
    {
      "name": "Person",
      "tableName": "catalog.mydb.Person",
      "idProperty": {"name": "id", "type": "int"},
      "properties": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
      ]
    },
    {
      "name": "Company",
      "tableName": "catalog.mydb.Company",
      "idProperty": {"name": "id", "type": "int"},
      "properties": [
        {"name": "name", "type": "string"},
        {"name": "industry", "type": "string"}
      ]
    }
  ],
  "edges": [
    {
      "name": "WORKS_AT",
      "sourceNode": "Person",
      "sinkNode": "Company",
      "tableName": "catalog.mydb.PersonWorksAt",
      "sourceIdProperty": {"name": "person_id", "type": "int"},
      "sinkIdProperty": {"name": "company_id", "type": "int"},
      "properties": [
        {"name": "since", "type": "int"}
      ]
    }
  ]
}
```

---

## Step 2: Write Your Query

Create a Cypher query in `my_query.cypher`:

```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE c.industry = 'Technology'
RETURN p.name, p.age, c.name AS company
ORDER BY p.age DESC
LIMIT 10
```

---

## Step 3: Transpile

Use the CLI to generate SQL:

```bash
gsql2rsql translate --schema my_schema.json < my_query.cypher
```

**Output:**
```sql
SELECT
   _gsql2rsql_p_name AS name
  ,_gsql2rsql_p_age AS age
  ,_gsql2rsql_c_name AS company
FROM (
  SELECT
     _left._gsql2rsql_p_id AS _gsql2rsql_p_id
    ,_left._gsql2rsql_p_name AS _gsql2rsql_p_name
    ,_left._gsql2rsql_p_age AS _gsql2rsql_p_age
    ,_left._gsql2rsql__anon1_person_id AS _gsql2rsql__anon1_person_id
    ,_left._gsql2rsql__anon1_company_id AS _gsql2rsql__anon1_company_id
    ,_right._gsql2rsql_c_id AS _gsql2rsql_c_id
    ,_right._gsql2rsql_c_name AS _gsql2rsql_c_name
    ,_right._gsql2rsql_c_industry AS _gsql2rsql_c_industry
  FROM (
    SELECT
       _left._gsql2rsql_p_id AS _gsql2rsql_p_id
      ,_left._gsql2rsql_p_name AS _gsql2rsql_p_name
      ,_left._gsql2rsql_p_age AS _gsql2rsql_p_age
      ,_right._gsql2rsql__anon1_person_id AS _gsql2rsql__anon1_person_id
      ,_right._gsql2rsql__anon1_company_id AS _gsql2rsql__anon1_company_id
    FROM (
      SELECT
         id AS _gsql2rsql_p_id
        ,name AS _gsql2rsql_p_name
        ,age AS _gsql2rsql_p_age
      FROM
        catalog.mydb.Person
    ) AS _left
    INNER JOIN (
      SELECT
         person_id AS _gsql2rsql__anon1_person_id
        ,company_id AS _gsql2rsql__anon1_company_id
      FROM
        catalog.mydb.PersonWorksAt
    ) AS _right
    ON (_left._gsql2rsql_p_id) = (_right._gsql2rsql__anon1_person_id)
  ) AS _left
  INNER JOIN (
    SELECT
       id AS _gsql2rsql_c_id
      ,name AS _gsql2rsql_c_name
      ,industry AS _gsql2rsql_c_industry
    FROM
      catalog.mydb.Company
  ) AS _right
  ON (_left._gsql2rsql__anon1_company_id) = (_right._gsql2rsql_c_id)
  WHERE (_gsql2rsql_c_industry) = ('Technology')
) AS _proj
ORDER BY _gsql2rsql_p_age DESC
LIMIT 10
```

---

## Step 4: Execute on Databricks

Save the SQL to a file and execute it:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gsql2rsql").getOrCreate()

# Read generated SQL
with open("output.sql") as f:
    sql = f.read()

# Execute
result = spark.sql(sql)
result.show()
```

---

## Understanding the Output

The generated SQL:

1. **Reads from tables**: `catalog.mydb.Person`, `catalog.mydb.PersonWorksAt`, `catalog.mydb.Company`
2. **Projects columns** with prefixed names (e.g., `_gsql2rsql_p_name`)
3. **Joins tables** based on relationship IDs
4. **Applies WHERE filter** on `c.industry = 'Technology'`
5. **Orders and limits** results

The prefixed column names (`_gsql2rsql_*`) avoid collisions with user column names.

---

## Next Steps

- [**Examples Gallery**](examples/index.md): See real-world queries
- [**CLI Commands**](cli-commands.md): Full command reference
- [**Query Translation**](query-translation.md): How it works
