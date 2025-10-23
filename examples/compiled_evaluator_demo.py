#!/usr/bin/env python
"""
Example demonstrating compiled expression evaluators in Draken.

This example shows how to use the evaluate() function with expression trees
to efficiently evaluate complex predicates over morsels. The compiled evaluator
optimizes common patterns like (x == 1 AND y == 'england') into single-pass
operations.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow as pa

import draken
from draken.evaluators import (
    BinaryExpression,
    ColumnExpression,
    LiteralExpression,
    evaluate,
)


def print_section(title):
    """Print a section header."""
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)
    print()


def main():
    """Demonstrate compiled expression evaluator usage."""
    
    print_section("Draken Compiled Expression Evaluator - Examples")
    
    # Create a sample morsel with multiple columns
    table = pa.table({
        'user_id': [1, 2, 3, 4, 5, 6, 7, 8],
        'age': [25, 30, 35, 40, 28, 32, 45, 29],
        'country': ['england', 'france', 'england', 'spain', 'england', 'france', 'spain', 'england'],
        'score': [85.5, 92.3, 78.1, 88.7, 95.2, 81.4, 73.9, 90.1]
    })
    morsel = draken.Morsel.from_arrow(table)
    
    print(f"Sample morsel: {morsel.num_rows} rows x {morsel.num_columns} columns")
    print(f"Columns: {morsel.column_names}")
    print()
    print("Data preview:")
    for i in range(min(5, morsel.num_rows)):
        row = morsel[i]
        print(f"  Row {i}: user_id={row[0]}, age={row[1]}, country={row[2]!r}, score={row[3]}")
    print()
    
    # Example 1: Simple comparison - age > 30
    print_section("Example 1: Simple Comparison (age > 30)")
    
    expr1 = BinaryExpression('greater_than', ColumnExpression('age'), LiteralExpression(30))
    result1 = evaluate(morsel, expr1)
    
    print("Expression: age > 30")
    print("Result:")
    for i in range(morsel.num_rows):
        age = morsel[i][1]
        match = result1[i]
        print(f"  Row {i}: age={age:2d} -> {match}")
    
    # Example 2: String comparison - country == 'england'
    print_section("Example 2: String Comparison (country == 'england')")
    
    expr2 = BinaryExpression('equals', ColumnExpression('country'), LiteralExpression('england'))
    result2 = evaluate(morsel, expr2)
    
    print("Expression: country == 'england'")
    print("Result:")
    for i in range(morsel.num_rows):
        country = morsel[i][2]
        match = result2[i]
        print(f"  Row {i}: country={country!r:12s} -> {match}")
    
    # Example 3: Compound AND expression - age > 30 AND country == 'england'
    print_section("Example 3: Compound AND (age > 30 AND country == 'england')")
    
    expr3_left = BinaryExpression('greater_than', ColumnExpression('age'), LiteralExpression(30))
    expr3_right = BinaryExpression('equals', ColumnExpression('country'), LiteralExpression('england'))
    expr3 = BinaryExpression('and', expr3_left, expr3_right)
    result3 = evaluate(morsel, expr3)
    
    print("Expression: age > 30 AND country == 'england'")
    print("Result:")
    for i in range(morsel.num_rows):
        age = morsel[i][1]
        country = morsel[i][2]
        match = result3[i]
        print(f"  Row {i}: age={age:2d}, country={country!r:12s} -> {match}")
    
    # Example 4: Compound OR expression
    print_section("Example 4: Compound OR (user_id == 1 OR user_id == 5)")
    
    expr4_left = BinaryExpression('equals', ColumnExpression('user_id'), LiteralExpression(1))
    expr4_right = BinaryExpression('equals', ColumnExpression('user_id'), LiteralExpression(5))
    expr4 = BinaryExpression('or', expr4_left, expr4_right)
    result4 = evaluate(morsel, expr4)
    
    print("Expression: user_id == 1 OR user_id == 5")
    print("Result:")
    for i in range(morsel.num_rows):
        user_id = morsel[i][0]
        match = result4[i]
        print(f"  Row {i}: user_id={user_id} -> {match}")
    
    # Example 5: Complex nested expression
    print_section("Example 5: Complex Nested Expression")
    
    # (age < 30 OR age > 40) AND score > 85
    age_lt_30 = BinaryExpression('less_than', ColumnExpression('age'), LiteralExpression(30))
    age_gt_40 = BinaryExpression('greater_than', ColumnExpression('age'), LiteralExpression(40))
    age_condition = BinaryExpression('or', age_lt_30, age_gt_40)
    score_condition = BinaryExpression('greater_than', ColumnExpression('score'), LiteralExpression(85.0))
    expr5 = BinaryExpression('and', age_condition, score_condition)
    
    result5 = evaluate(morsel, expr5)
    
    print("Expression: (age < 30 OR age > 40) AND score > 85")
    print("Result:")
    for i in range(morsel.num_rows):
        age = morsel[i][1]
        score = morsel[i][3]
        match = result5[i]
        print(f"  Row {i}: age={age:2d}, score={score:5.1f} -> {match}")
    
    # Example 6: Vector-vector comparison
    print_section("Example 6: Vector-Vector Comparison")
    
    # Create a morsel with two numeric columns to compare
    table2 = pa.table({
        'a': [1, 2, 3, 4, 5],
        'b': [1, 3, 3, 2, 6]
    })
    morsel2 = draken.Morsel.from_arrow(table2)
    
    expr6 = BinaryExpression('equals', ColumnExpression('a'), ColumnExpression('b'))
    result6 = evaluate(morsel2, expr6)
    
    print("Expression: a == b")
    print("Result:")
    for i in range(morsel2.num_rows):
        a = morsel2[i][0]
        b = morsel2[i][1]
        match = result6[i]
        print(f"  Row {i}: a={a}, b={b} -> {match}")
    
    # Example 7: Demonstrating caching
    print_section("Example 7: Evaluator Caching")
    
    from draken.evaluators.evaluator import _evaluator_cache, clear_cache
    
    clear_cache()
    print(f"Cache size after clearing: {len(_evaluator_cache)}")
    
    # First evaluation
    expr7 = BinaryExpression('equals', ColumnExpression('user_id'), LiteralExpression(1))
    evaluate(morsel, expr7)
    print(f"Cache size after first evaluation: {len(_evaluator_cache)}")
    
    # Second evaluation of same expression (should reuse cached evaluator)
    evaluate(morsel, expr7)
    print(f"Cache size after second evaluation: {len(_evaluator_cache)}")
    
    # Different expression
    expr7b = BinaryExpression('equals', ColumnExpression('user_id'), LiteralExpression(2))
    evaluate(morsel, expr7b)
    print(f"Cache size after different expression: {len(_evaluator_cache)}")
    
    print()
    print("=" * 70)
    print("Examples completed successfully!")
    print("=" * 70)
    print()
    print("Key Features:")
    print("  ✓ Simple comparisons (column vs literal)")
    print("  ✓ Vector-vector comparisons (column vs column)")
    print("  ✓ Compound expressions (AND, OR)")
    print("  ✓ Nested expressions")
    print("  ✓ Automatic caching for performance")
    print("=" * 70)


if __name__ == "__main__":
    main()
