#!/usr/bin/env python
"""
Visualize expression trees and their compiled evaluation.

This script demonstrates how expression trees are structured and
how they get compiled into efficient evaluators.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from draken.evaluators.expression import (
    BinaryExpression,
    ColumnExpression,
    LiteralExpression,
    UnaryExpression,
)


def print_tree(expr, indent=0, label=""):
    """Print expression tree with indentation."""
    prefix = "  " * indent
    
    if isinstance(expr, LiteralExpression):
        print(f"{prefix}{label}Literal({expr.value!r})")
    
    elif isinstance(expr, ColumnExpression):
        print(f"{prefix}{label}Column({expr.column_name!r})")
    
    elif isinstance(expr, BinaryExpression):
        print(f"{prefix}{label}{expr.operation.upper()}")
        print_tree(expr.left, indent + 1, "├─ ")
        print_tree(expr.right, indent + 1, "└─ ")
    
    elif isinstance(expr, UnaryExpression):
        print(f"{prefix}{label}{expr.operation.upper()}")
        print_tree(expr.operand, indent + 1, "└─ ")


def show_compilation_pattern(expr, pattern_name):
    """Show how an expression gets compiled."""
    print(f"\n{pattern_name}")
    print("─" * 70)
    print("\nExpression Tree:")
    print_tree(expr)
    print("\nCompiled Pattern:")
    
    if isinstance(expr, BinaryExpression):
        if expr.operation == 'equals':
            if isinstance(expr.left, ColumnExpression) and isinstance(expr.right, LiteralExpression):
                print(f"  Optimized: morsel.column('{expr.left.column_name}').equals({expr.right.value!r})")
            elif isinstance(expr.left, ColumnExpression) and isinstance(expr.right, ColumnExpression):
                print(f"  Optimized: morsel.column('{expr.left.column_name}').equals_vector(morsel.column('{expr.right.column_name}'))")
        
        elif expr.operation == 'and':
            print(f"  Optimized: evaluate(left).and_vector(evaluate(right))")
            print(f"  Note: Evaluates both sub-expressions and combines results")


def main():
    """Demonstrate expression tree visualization."""
    
    print("=" * 70)
    print("Expression Tree Visualization")
    print("=" * 70)
    
    # Example 1: Simple comparison
    expr1 = BinaryExpression('equals', ColumnExpression('age'), LiteralExpression(30))
    show_compilation_pattern(expr1, "Example 1: Column == Literal (age == 30)")
    
    # Example 2: Column comparison
    expr2 = BinaryExpression('equals', ColumnExpression('price'), ColumnExpression('cost'))
    show_compilation_pattern(expr2, "Example 2: Column == Column (price == cost)")
    
    # Example 3: Compound AND
    print("\nExample 3: Compound AND (x == 1 AND y == 'england')")
    print("─" * 70)
    
    left = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
    right = BinaryExpression('equals', ColumnExpression('y'), LiteralExpression('england'))
    expr3 = BinaryExpression('and', left, right)
    
    print("\nExpression Tree:")
    print_tree(expr3)
    
    print("\nCompiled Pattern:")
    print("  Step 1: temp1 = morsel.column('x').equals(1)")
    print("  Step 2: temp2 = morsel.column('y').equals('england')")
    print("  Step 3: result = temp1.and_vector(temp2)")
    print("  Optimized: Single-pass combination of results")
    
    # Example 4: Complex nested
    print("\nExample 4: Complex Nested ((age < 30 OR age > 60) AND income > 50000)")
    print("─" * 70)
    
    age_lt = BinaryExpression('less_than', ColumnExpression('age'), LiteralExpression(30))
    age_gt = BinaryExpression('greater_than', ColumnExpression('age'), LiteralExpression(60))
    age_cond = BinaryExpression('or', age_lt, age_gt)
    income_cond = BinaryExpression('greater_than', ColumnExpression('income'), LiteralExpression(50000))
    expr4 = BinaryExpression('and', age_cond, income_cond)
    
    print("\nExpression Tree:")
    print_tree(expr4)
    
    print("\nCompiled Pattern:")
    print("  Step 1: temp1 = morsel.column('age').less_than(30)")
    print("  Step 2: temp2 = morsel.column('age').greater_than(60)")
    print("  Step 3: temp3 = temp1.or_vector(temp2)")
    print("  Step 4: temp4 = morsel.column('income').greater_than(50000)")
    print("  Step 5: result = temp3.and_vector(temp4)")
    print("  Optimized: Efficient combination of vector operations")
    
    # Example 5: Unary NOT
    print("\nExample 5: Unary NOT (NOT active)")
    print("─" * 70)
    
    expr5 = UnaryExpression('not', ColumnExpression('active'))
    
    print("\nExpression Tree:")
    print_tree(expr5)
    
    print("\nCompiled Pattern:")
    print("  Optimized: morsel.column('active').not_()")
    
    print("\n" + "=" * 70)
    print("Key Insights:")
    print("=" * 70)
    print()
    print("1. Expression trees provide clean separation of concerns:")
    print("   - Tree structure represents query logic")
    print("   - Compilation generates efficient execution code")
    print()
    print("2. Pattern matching enables optimization:")
    print("   - Column vs Literal → Direct vector operation")
    print("   - Column vs Column → Vector-vector operation")
    print("   - Compound expressions → Efficient combination")
    print()
    print("3. Caching amortizes compilation cost:")
    print("   - Same structure, different literals → Cache hit")
    print("   - Repeated queries benefit from cached evaluators")
    print()
    print("4. Extensible design:")
    print("   - Easy to add new operations")
    print("   - Can add specialized optimizations for hot patterns")
    print("=" * 70)


if __name__ == "__main__":
    main()
