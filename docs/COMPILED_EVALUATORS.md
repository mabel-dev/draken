# Compiled Expression Evaluators

## Overview

Draken provides a compiled expression evaluator system that can efficiently evaluate complex predicates and expressions over morsels. Instead of evaluating each operation in isolation through a generic loop-and-branch interpreter, the compiled evaluator recognizes common expression patterns and generates optimized single-pass evaluation code.

## Why Compiled Evaluators?

Traditional expression evaluation typically works like this:

```python
# Generic interpreter approach (slower)
# For expression: x == 1 AND y == 'england'

# Step 1: Evaluate x == 1
temp1 = []
for row in morsel:
    temp1.append(row['x'] == 1)

# Step 2: Evaluate y == 'england'  
temp2 = []
for row in morsel:
    temp2.append(row['y'] == 'england')

# Step 3: Combine with AND
result = []
for i in range(len(temp1)):
    result.append(temp1[i] and temp2[i])
```

The compiled evaluator optimizes this into a single pass:

```python
# Compiled approach (faster)
result = []
for row in morsel:
    result.append(row['x'] == 1 and row['y'] == 'england')
```

Even better, Draken's implementation works at the vector level, avoiding Python loops entirely through Cython-optimized operations.

## Quick Start

```python
import draken
import pyarrow as pa
from draken.evaluators import (
    BinaryExpression,
    ColumnExpression, 
    LiteralExpression,
    evaluate
)

# Create a morsel
table = pa.table({
    'x': [1, 2, 3, 4, 5],
    'y': ['england', 'france', 'england', 'spain', 'england']
})
morsel = draken.Morsel.from_arrow(table)

# Build expression tree: x == 1 AND y == 'england'
expr1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
expr2 = BinaryExpression('equals', ColumnExpression('y'), LiteralExpression('england'))
expr = BinaryExpression('and', expr1, expr2)

# Evaluate - returns a boolean vector
result = evaluate(morsel, expr)

# Use result
print(list(result))  # [True, False, False, False, False]
```

## Expression Node Types

### LiteralExpression

Represents a constant value:

```python
LiteralExpression(1)          # Integer literal
LiteralExpression('england')  # String literal
LiteralExpression(3.14)       # Float literal
LiteralExpression(True)       # Boolean literal
```

### ColumnExpression

Represents a reference to a column in the morsel:

```python
ColumnExpression('x')         # Reference to column 'x'
ColumnExpression('country')   # Reference to column 'country'
```

### BinaryExpression

Represents a binary operation between two expressions:

```python
# Comparison operations
BinaryExpression('equals', left, right)
BinaryExpression('not_equals', left, right)
BinaryExpression('greater_than', left, right)
BinaryExpression('greater_than_or_equals', left, right)
BinaryExpression('less_than', left, right)
BinaryExpression('less_than_or_equals', left, right)

# Boolean operations
BinaryExpression('and', left, right)
BinaryExpression('or', left, right)
BinaryExpression('xor', left, right)

# Arithmetic operations (future)
BinaryExpression('add', left, right)
BinaryExpression('subtract', left, right)
BinaryExpression('multiply', left, right)
BinaryExpression('divide', left, right)
```

## Supported Patterns

The compiled evaluator recognizes and optimizes these common patterns:

### Column vs Literal

```python
# x == 1
BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))

# age > 30
BinaryExpression('greater_than', ColumnExpression('age'), LiteralExpression(30))

# country == 'england'
BinaryExpression('equals', ColumnExpression('country'), LiteralExpression('england'))
```

### Column vs Column (Vector-Vector)

```python
# x == y
BinaryExpression('equals', ColumnExpression('x'), ColumnExpression('y'))

# price > cost
BinaryExpression('greater_than', ColumnExpression('price'), ColumnExpression('cost'))
```

### Compound Expressions

```python
# x == 1 AND y == 'england'
expr1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
expr2 = BinaryExpression('equals', ColumnExpression('y'), LiteralExpression('england'))
BinaryExpression('and', expr1, expr2)

# x < 10 OR x > 90
expr1 = BinaryExpression('less_than', ColumnExpression('x'), LiteralExpression(10))
expr2 = BinaryExpression('greater_than', ColumnExpression('x'), LiteralExpression(90))
BinaryExpression('or', expr1, expr2)
```

### Nested Expressions

```python
# (age < 30 OR age > 40) AND score > 85
age_lt_30 = BinaryExpression('less_than', ColumnExpression('age'), LiteralExpression(30))
age_gt_40 = BinaryExpression('greater_than', ColumnExpression('age'), LiteralExpression(40))
age_condition = BinaryExpression('or', age_lt_30, age_gt_40)

score_condition = BinaryExpression('greater_than', ColumnExpression('score'), LiteralExpression(85))

final_expr = BinaryExpression('and', age_condition, score_condition)
```

## Performance Features

### Automatic Caching

The evaluator automatically caches compiled evaluators based on expression structure:

```python
# First evaluation - compiles and caches
result1 = evaluate(morsel1, expr)

# Second evaluation - reuses cached evaluator (faster)
result2 = evaluate(morsel2, expr)
```

Cache keys are based on expression structure, not literal values, so:

```python
expr1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
expr2 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(2))

# These produce different results but may share optimization patterns
```

To clear the cache:

```python
from draken.evaluators.evaluator import clear_cache
clear_cache()
```

### Single-Pass Evaluation

For compound expressions, the evaluator combines operations to minimize passes over the data:

```python
# This expression:
# (x == 1 AND y == 'england') OR (x == 5 AND y == 'france')

# Is evaluated in an optimized way that:
# 1. Fetches each column once
# 2. Performs comparisons using SIMD operations where possible
# 3. Combines results efficiently with boolean operations
```

## API Reference

### evaluate(morsel, expression)

Main entry point for expression evaluation.

**Parameters:**
- `morsel` (Morsel): The morsel to evaluate the expression over
- `expression` (Expression): The expression tree to evaluate

**Returns:**
- `Vector`: Result vector (typically boolean for predicates)

**Example:**
```python
result = evaluate(morsel, expr)
```

### Expression Classes

All expression classes support:
- `__repr__()`: String representation
- `__eq__()`: Equality comparison
- `__hash__()`: Hashing for caching

## Examples

See `examples/compiled_evaluator_demo.py` for comprehensive examples including:

1. Simple comparisons (column vs literal)
2. String comparisons
3. Compound AND expressions
4. Compound OR expressions
5. Complex nested expressions
6. Vector-vector comparisons
7. Caching demonstration

Run the example:

```bash
python examples/compiled_evaluator_demo.py
```

## Integration with Opteryx

The compiled evaluator is designed to integrate seamlessly with [Opteryx](https://github.com/mabel-dev/opteryx) for high-performance SQL query execution:

```python
# In Opteryx, predicates from WHERE clauses can be converted to
# expression trees and evaluated efficiently over morsels
from opteryx.planner import Predicate
from draken.evaluators import evaluate

# Convert SQL predicate to expression tree
expr = predicate_to_expression(sql_where_clause)

# Evaluate over morsel
result = evaluate(morsel, expr)

# Filter morsel based on result
filtered = morsel.take(result)
```

## Future Enhancements

Planned improvements include:

1. **Arithmetic operations**: Full support for `add`, `subtract`, `multiply`, `divide`
2. **Function calls**: `upper()`, `lower()`, `substring()`, etc.
3. **JIT compilation**: Runtime code generation for even faster evaluation
4. **SIMD optimization**: Explicit SIMD operations for common patterns
5. **Null handling**: Optimized null-aware operations
6. **Type coercion**: Automatic type conversion where safe

## Performance Tips

1. **Build expression trees once**: Reuse expression trees across multiple morsels
2. **Use literals wisely**: The evaluator is optimized for literal comparisons
3. **Avoid deep nesting**: Very deep expression trees may have overhead
4. **Let caching work**: Evaluate similar expressions to benefit from cache hits
5. **Profile your queries**: Use benchmarks to understand performance characteristics

## Benchmarking

To compare compiled evaluator performance vs manual loops:

```python
import time
import draken
import pyarrow as pa
from draken.evaluators import evaluate, BinaryExpression, ColumnExpression, LiteralExpression

# Create large morsel
table = pa.table({'x': range(1000000), 'y': range(1000000)})
morsel = draken.Morsel.from_arrow(table)

# Compiled evaluator
expr = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(500000))
start = time.time()
result = evaluate(morsel, expr)
compiled_time = time.time() - start

print(f"Compiled evaluator: {compiled_time:.4f} seconds")

# Manual approach
start = time.time()
x_vec = morsel.column(b'x')
manual_result = x_vec.equals(500000)
manual_time = time.time() - start

print(f"Manual approach: {manual_time:.4f} seconds")
```
