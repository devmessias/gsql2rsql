# Functions Reference

Supported Cypher functions transpiled to Databricks SQL, plus custom directives for graph traversal control.

---

## gsql2rsql Extensions

These functions are **not part of standard OpenCypher** — they are custom directives provided by gsql2rsql.

### `is_terminator(predicate)`

Traversal barrier directive for variable-length path (VLP) queries. Nodes matching the predicate **are reached** (included in results) but BFS does **not expand from them** — they act as a barrier.

!!! warning "VLP only"
    `is_terminator()` can only be used inside variable-length path queries (`-[*1..N]->`). Using it in a regular MATCH raises an error.

**Syntax:**

```cypher
MATCH path = (a:Type)-[:REL*1..N]->(b:Type)
WHERE is_terminator(b.property = value)
RETURN b
```

**Examples:**

=== "Basic barrier"

    ```cypher
    -- Stop BFS at hub stations, but include them in results
    MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
    WHERE a.node_id = 'N1'
      AND is_terminator(b.is_hub = true)
    RETURN DISTINCT b.node_id AS dst
    ```

=== "With edge filter"

    ```cypher
    -- Combine barrier with edge filter
    MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
    WHERE a.node_id = 'N1'
      AND is_terminator(b.is_hub = true)
      AND ALL(r IN relationships(path) WHERE r.weight > 0)
    RETURN DISTINCT b.node_id AS dst
    ```

=== "With sink filter"

    ```cypher
    -- Barrier + filter on returned nodes
    MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
    WHERE a.node_id = 'N1'
      AND b.is_hub = true
      AND is_terminator(b.is_hub = true)
    RETURN DISTINCT b.node_id AS dst
    ```

**Semantics:**

| Aspect | Behavior |
|---|---|
| Barrier nodes reached? | Yes — edges **to** them are in results |
| Barrier nodes expanded? | No — BFS stops expanding **from** them |
| Works with CTE renderer? | Yes — `NOT EXISTS` in recursive step |
| Works with procedural BFS? | Yes — `NOT EXISTS` in frontier creation |
| Bidirectional BFS? | Barrier applies to forward direction only |

---

## Path Functions

Functions for working with paths from variable-length traversals.

| Cypher | Databricks SQL | Description |
|---|---|---|
| `nodes(path)` | Array of node IDs | Returns the node IDs along a path |
| `relationships(path)` | Array of edge structs | Returns the edges along a path |
| `length(path)` | `SIZE(path) - 1` | Number of edges (hops) in a path |

```cypher
MATCH path = (a)-[:KNOWS*1..3]->(b)
WHERE a.node_id = 'Alice'
RETURN nodes(path), relationships(path), length(path)
```

---

## String Functions

| Cypher | Databricks SQL | Description |
|---|---|---|
| `toUpper(s)` | `UPPER(s)` | Convert to uppercase |
| `toLower(s)` | `LOWER(s)` | Convert to lowercase |
| `trim(s)` | `TRIM(s)` | Remove leading/trailing whitespace |
| `lTrim(s)` | `LTRIM(s)` | Remove leading whitespace |
| `rTrim(s)` | `RTRIM(s)` | Remove trailing whitespace |
| `size(s)` | `LENGTH(s)` | String length |
| `left(s, n)` | `LEFT(s, n)` | First n characters |
| `right(s, n)` | `RIGHT(s, n)` | Last n characters |
| `startsWith(s, prefix)` | `STARTSWITH(s, prefix)` | Check prefix |
| `endsWith(s, suffix)` | `ENDSWITH(s, suffix)` | Check suffix |
| `contains(s, sub)` | `CONTAINS(s, sub)` | Check substring |

---

## Type Conversion Functions

| Cypher | Databricks SQL | Description |
|---|---|---|
| `toString(x)` | `CAST(x AS STRING)` | Convert to string |
| `toInteger(x)` | `CAST(x AS BIGINT)` | Convert to integer |
| `toLong(x)` | `CAST(x AS BIGINT)` | Convert to long integer |
| `toFloat(x)` | `CAST(x AS DOUBLE)` | Convert to float |
| `toDouble(x)` | `CAST(x AS DOUBLE)` | Convert to double |
| `toBoolean(x)` | `CAST(x AS BOOLEAN)` | Convert to boolean |

---

## Math Functions

### Unary

| Cypher | Databricks SQL | Description |
|---|---|---|
| `abs(x)` | `ABS(x)` | Absolute value |
| `ceil(x)` / `ceiling(x)` | `CEIL(x)` | Round up |
| `floor(x)` | `FLOOR(x)` | Round down |
| `round(x)` / `round(x, p)` | `ROUND(x)` / `ROUND(x, p)` | Round to nearest (optional precision) |
| `sqrt(x)` | `SQRT(x)` | Square root |
| `sign(x)` | `SIGN(x)` | Sign (-1, 0, 1) |
| `log(x)` / `ln(x)` | `LN(x)` | Natural logarithm |
| `log10(x)` | `LOG10(x)` | Base-10 logarithm |
| `exp(x)` | `EXP(x)` | Euler's number raised to x |

### Trigonometric

| Cypher | Databricks SQL | Description |
|---|---|---|
| `sin(x)` | `SIN(x)` | Sine |
| `cos(x)` | `COS(x)` | Cosine |
| `tan(x)` | `TAN(x)` | Tangent |
| `asin(x)` | `ASIN(x)` | Arc sine |
| `acos(x)` | `ACOS(x)` | Arc cosine |
| `atan(x)` | `ATAN(x)` | Arc tangent |
| `atan2(y, x)` | `ATAN2(y, x)` | Two-argument arc tangent |
| `degrees(x)` | `DEGREES(x)` | Radians to degrees |
| `radians(x)` | `RADIANS(x)` | Degrees to radians |

### Constants

| Cypher | Databricks SQL | Description |
|---|---|---|
| `rand()` | `RAND()` | Random number [0, 1) |
| `pi()` | `PI()` | Pi constant |
| `e()` | `E()` | Euler's number |

---

## Null Handling

| Cypher | Databricks SQL | Description |
|---|---|---|
| `x IS NULL` | `(x) IS NULL` | Check if null |
| `x IS NOT NULL` | `(x) IS NOT NULL` | Check if not null |
| `coalesce(a, b, ...)` | `COALESCE(a, b, ...)` | First non-null value (variadic) |

---

## Collection Functions

| Cypher | Databricks SQL | Description |
|---|---|---|
| `size(list)` | `SIZE(list)` | Number of elements in list |
| `range(start, end)` | `SEQUENCE(start, end)` | Generate integer list |
| `range(start, end, step)` | `SEQUENCE(start, end, step)` | Generate integer list with step |

---

## Date/Time Functions

### Constructors

These functions create date/time values from components or strings. They use complex rendering logic (not simple templates).

| Cypher | Description |
|---|---|
| `date()` | Current date |
| `date({year: y, month: m, day: d})` | Date from components |
| `datetime()` | Current datetime |
| `datetime({year: y, month: m, ...})` | Datetime from components |
| `time()` | Current time |
| `localtime()` | Current local time |
| `localdatetime()` | Current local datetime |
| `duration({days: d, hours: h, ...})` | Duration from components |

### Component Extraction

| Cypher | Databricks SQL | Description |
|---|---|---|
| `d.year` | `YEAR(d)` | Extract year |
| `d.month` | `MONTH(d)` | Extract month |
| `d.day` | `DAY(d)` | Extract day |
| `d.hour` | `HOUR(d)` | Extract hour |
| `d.minute` | `MINUTE(d)` | Extract minute |
| `d.second` | `SECOND(d)` | Extract second |
| `d.week` | `WEEKOFYEAR(d)` | Extract week of year |
| `d.dayOfWeek` | `DAYOFWEEK(d)` | Extract day of week |
| `d.quarter` | `QUARTER(d)` | Extract quarter |

### Date Arithmetic

| Cypher | Databricks SQL | Description |
|---|---|---|
| `date.truncate(unit, d)` | `DATE_TRUNC(unit, d)` | Truncate to unit |
| `duration.between(d1, d2)` | `DATEDIFF(d2, d1)` | Days between dates |

!!! note "Parameter swap"
    `duration.between(d1, d2)` renders as `DATEDIFF(d2, d1)` — parameters are swapped to match Databricks semantics.

---

## List Predicates

These are used with `ALL`, `ANY`, `NONE`, `SINGLE` for filtering over collections.

```cypher
-- Filter all edges in a path
MATCH path = (a)-[:KNOWS*1..3]->(b)
WHERE ALL(r IN relationships(path) WHERE r.weight > 0)
RETURN b

-- Check if any node has a property
MATCH path = (a)-[:KNOWS*1..3]->(b)
WHERE ANY(n IN nodes(path) WHERE n = 'target_id')
RETURN b
```

| Predicate | Description |
|---|---|
| `ALL(x IN list WHERE pred)` | True if predicate holds for all elements |
| `ANY(x IN list WHERE pred)` | True if predicate holds for at least one element |
| `NONE(x IN list WHERE pred)` | True if predicate holds for no elements |
| `SINGLE(x IN list WHERE pred)` | True if predicate holds for exactly one element |

---

## Aggregation Functions

Used in `RETURN` or `WITH` clauses for grouping and summarizing data.

| Cypher | Databricks SQL | Description |
|---|---|---|
| `count(x)` | `COUNT(x)` | Count values |
| `sum(x)` | `SUM(x)` | Sum values |
| `avg(x)` | `AVG(CAST(x AS DOUBLE))` | Average (auto-cast to double) |
| `min(x)` | `MIN(x)` | Minimum value |
| `max(x)` | `MAX(x)` | Maximum value |
| `first(x)` | `FIRST(x)` | First value in group |
| `last(x)` | `LAST(x)` | Last value in group |
| `stDev(x)` | `STDDEV(x)` | Standard deviation (sample) |
| `stDevP(x)` | `STDDEV_POP(x)` | Standard deviation (population) |
| `collect(x)` | `COLLECT_LIST(x)` | Collect values into list |

---

## Operators

### Arithmetic

| Cypher | SQL | Description |
|---|---|---|
| `a + b` | `(a) + (b)` | Addition |
| `a - b` | `(a) - (b)` | Subtraction |
| `a * b` | `(a) * (b)` | Multiplication |
| `a / b` | `(a) / (b)` | Division |
| `a % b` | `(a) % (b)` | Modulo |
| `a ^ b` | `POWER(a, b)` | Exponentiation |

### Comparison

| Cypher | SQL | Description |
|---|---|---|
| `a = b` | `(a) = (b)` | Equality |
| `a <> b` | `(a) != (b)` | Inequality |
| `a < b` | `(a) < (b)` | Less than |
| `a <= b` | `(a) <= (b)` | Less or equal |
| `a > b` | `(a) > (b)` | Greater than |
| `a >= b` | `(a) >= (b)` | Greater or equal |
| `a =~ regex` | `(a) RLIKE (regex)` | Regex match |
| `a IN list` | `(a) IN list` | List membership |

### Logical

| Cypher | SQL | Description |
|---|---|---|
| `a AND b` | `(a) AND (b)` | Logical AND |
| `a OR b` | `(a) OR (b)` | Logical OR |
| `a XOR b` | `((a) AND NOT (b)) OR (NOT (a) AND (b))` | Logical XOR |
| `NOT a` | `NOT (a)` | Logical NOT |
