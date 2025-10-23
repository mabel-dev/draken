"""
Compiled expression evaluator for Draken.

This module provides the main evaluate() function that takes a morsel and
an expression tree, and evaluates the expression efficiently using compiled
evaluators for common patterns.
"""

from typing import Any, Callable, Dict, Tuple
import hashlib

from draken.evaluators.expression import (
    BinaryExpression,
    ColumnExpression,
    Expression,
    LiteralExpression,
    UnaryExpression,
)
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector


class CompiledEvaluator:
    """
    A compiled evaluator for a specific expression pattern.
    
    Compiled evaluators are cached and reused for common expression patterns
    to avoid repeated interpretation overhead.
    """
    
    def __init__(self, expression: Expression, evaluator_func: Callable):
        """
        Create a compiled evaluator.
        
        Args:
            expression: The expression pattern this evaluator handles
            evaluator_func: The compiled evaluation function
        """
        self.expression = expression
        self.evaluator_func = evaluator_func
    
    def evaluate(self, morsel: Morsel) -> Vector:
        """
        Evaluate the expression over a morsel.
        
        Args:
            morsel: The morsel to evaluate over
            
        Returns:
            Vector: Result vector
        """
        return self.evaluator_func(morsel)


# Cache for compiled evaluators
_evaluator_cache: Dict[int, CompiledEvaluator] = {}


def _get_expression_hash(expr: Expression) -> int:
    """Get a hash for an expression to use for caching."""
    return hash(expr)


def _compile_binary_comparison(operation: str, left: Expression, right: Expression) -> Callable:
    """
    Compile a binary comparison operation into an optimized evaluator.
    
    This generates an optimized single-pass evaluator for common patterns like:
    - column == literal
    - column > literal
    - column1 == column2
    """
    
    # Pattern: column OP literal (e.g., x == 1)
    if isinstance(left, ColumnExpression) and isinstance(right, LiteralExpression):
        col_name = left.column_name
        literal_value = right.value
        
        def evaluator(morsel: Morsel) -> Vector:
            # Get the column vector
            col_bytes = col_name.encode('utf-8')
            vec = morsel.column(col_bytes)
            
            # Call the appropriate vector comparison method
            if operation == 'equals':
                return vec.equals(literal_value)
            elif operation == 'not_equals':
                return vec.not_equals(literal_value)
            elif operation == 'greater_than':
                return vec.greater_than(literal_value)
            elif operation == 'greater_than_or_equals':
                return vec.greater_than_or_equals(literal_value)
            elif operation == 'less_than':
                return vec.less_than(literal_value)
            elif operation == 'less_than_or_equals':
                return vec.less_than_or_equals(literal_value)
            else:
                raise ValueError(f"Unknown comparison operation: {operation}")
        
        return evaluator
    
    # Pattern: column1 OP column2 (e.g., x == y)
    elif isinstance(left, ColumnExpression) and isinstance(right, ColumnExpression):
        left_col = left.column_name
        right_col = right.column_name
        
        def evaluator(morsel: Morsel) -> Vector:
            # Get both column vectors
            left_vec = morsel.column(left_col.encode('utf-8'))
            right_vec = morsel.column(right_col.encode('utf-8'))
            
            # Call the appropriate vector-vector comparison method
            if operation == 'equals':
                return left_vec.equals_vector(right_vec)
            elif operation == 'not_equals':
                return left_vec.not_equals_vector(right_vec)
            elif operation == 'greater_than':
                return left_vec.greater_than_vector(right_vec)
            elif operation == 'greater_than_or_equals':
                return left_vec.greater_than_or_equals_vector(right_vec)
            elif operation == 'less_than':
                return left_vec.less_than_vector(right_vec)
            elif operation == 'less_than_or_equals':
                return left_vec.less_than_or_equals_vector(right_vec)
            else:
                raise ValueError(f"Unknown comparison operation: {operation}")
        
        return evaluator
    
    # Fall back to generic evaluation
    return None


def _compile_binary_boolean(operation: str, left: Expression, right: Expression) -> Callable:
    """
    Compile a binary boolean operation (AND, OR, XOR) into an optimized evaluator.
    
    For compound expressions like (x == 1 AND y == 'england'), this can evaluate
    both conditions in a single pass and combine them efficiently.
    """
    
    def evaluator(morsel: Morsel) -> Vector:
        # Evaluate left and right sub-expressions
        left_result = evaluate(morsel, left)
        right_result = evaluate(morsel, right)
        
        # Combine using boolean operation
        if operation == 'and':
            return left_result.and_vector(right_result)
        elif operation == 'or':
            return left_result.or_vector(right_result)
        elif operation == 'xor':
            return left_result.xor_vector(right_result)
        else:
            raise ValueError(f"Unknown boolean operation: {operation}")
    
    return evaluator


def _compile_expression(expr: Expression) -> CompiledEvaluator:
    """
    Compile an expression into an optimized evaluator.
    
    This function analyzes the expression pattern and generates the most
    efficient evaluation strategy.
    """
    
    # Handle literal expressions
    if isinstance(expr, LiteralExpression):
        def evaluator(morsel: Morsel) -> Any:
            return expr.value
        return CompiledEvaluator(expr, evaluator)
    
    # Handle column expressions
    if isinstance(expr, ColumnExpression):
        def evaluator(morsel: Morsel) -> Vector:
            col_bytes = expr.column_name.encode('utf-8')
            return morsel.column(col_bytes)
        return CompiledEvaluator(expr, evaluator)
    
    # Handle binary expressions
    if isinstance(expr, BinaryExpression):
        # Try to compile as comparison
        comparison_ops = ['equals', 'not_equals', 'greater_than', 
                         'greater_than_or_equals', 'less_than', 'less_than_or_equals']
        if expr.operation in comparison_ops:
            compiled = _compile_binary_comparison(expr.operation, expr.left, expr.right)
            if compiled:
                return CompiledEvaluator(expr, compiled)
        
        # Try to compile as boolean operation
        boolean_ops = ['and', 'or', 'xor']
        if expr.operation in boolean_ops:
            compiled = _compile_binary_boolean(expr.operation, expr.left, expr.right)
            if compiled:
                return CompiledEvaluator(expr, compiled)
        
        # Fall back to generic evaluation
        def evaluator(morsel: Morsel) -> Vector:
            left_result = evaluate(morsel, expr.left)
            right_result = evaluate(morsel, expr.right)
            
            # Determine if operands are scalars
            left_is_scalar = not isinstance(left_result, Vector)
            right_is_scalar = not isinstance(right_result, Vector)
            
            # For now, raise error on unsupported operations
            raise ValueError(f"Operation {expr.operation} not yet supported in generic evaluator")
        
        return CompiledEvaluator(expr, evaluator)
    
    # Handle unary expressions
    if isinstance(expr, UnaryExpression):
        def evaluator(morsel: Morsel) -> Vector:
            operand_result = evaluate(morsel, expr.operand)
            
            if expr.operation == 'not':
                return operand_result.not_()
            elif expr.operation == 'is_null':
                # Would need to implement is_null on vectors
                raise ValueError("is_null operation not yet implemented")
            else:
                raise ValueError(f"Unknown unary operation: {expr.operation}")
        
        return CompiledEvaluator(expr, evaluator)
    
    raise ValueError(f"Unknown expression type: {type(expr)}")


def evaluate(morsel: Morsel, expression: Expression) -> Vector:
    """
    Evaluate an expression tree over a morsel.
    
    This is the main entry point for expression evaluation. It uses compiled
    evaluators for common patterns to achieve high performance.
    
    Args:
        morsel: The morsel to evaluate the expression over
        expression: The expression tree to evaluate
        
    Returns:
        Vector: Result vector (typically a boolean vector for predicates)
        
    Examples:
        >>> import draken
        >>> import pyarrow as pa
        >>> from draken.evaluators import (
        ...     evaluate, BinaryExpression, ColumnExpression, LiteralExpression
        ... )
        >>> 
        >>> # Create a morsel
        >>> table = pa.table({'x': [1, 2, 3, 4, 5], 'y': [10, 20, 30, 40, 50]})
        >>> morsel = draken.Morsel.from_arrow(table)
        >>> 
        >>> # Create expression: x == 3
        >>> expr = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(3))
        >>> 
        >>> # Evaluate
        >>> result = evaluate(morsel, expr)
        >>> print(list(result))
        [False, False, True, False, False]
        
        >>> # Create compound expression: x == 3 AND y > 20
        >>> expr1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(3))
        >>> expr2 = BinaryExpression('greater_than', ColumnExpression('y'), LiteralExpression(20))
        >>> compound = BinaryExpression('and', expr1, expr2)
        >>> 
        >>> # Evaluate
        >>> result = evaluate(morsel, compound)
        >>> print(list(result))
        [False, False, True, False, False]
    """
    # Check cache first
    expr_hash = _get_expression_hash(expression)
    
    if expr_hash in _evaluator_cache:
        compiled = _evaluator_cache[expr_hash]
    else:
        # Compile the expression
        compiled = _compile_expression(expression)
        
        # Cache it for reuse
        _evaluator_cache[expr_hash] = compiled
    
    # Evaluate and return
    return compiled.evaluate(morsel)


def clear_cache():
    """Clear the compiled evaluator cache."""
    global _evaluator_cache
    _evaluator_cache.clear()
