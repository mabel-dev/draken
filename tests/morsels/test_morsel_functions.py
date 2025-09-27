#!/usr/bin/env python
"""Tests for new Morsel methods: take, select, rename, to_arrow."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest
import pyarrow as pa
import draken


def test_methods_exist():
    """Test that all required methods exist on the Morsel class."""
    table = pa.table({'a': [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Check that all methods exist
    assert hasattr(morsel, 'take')
    assert hasattr(morsel, 'select')
    assert hasattr(morsel, 'rename')
    assert hasattr(morsel, 'to_arrow')
    
    # Check that methods are callable
    assert callable(morsel.take)
    assert callable(morsel.select)
    assert callable(morsel.rename)
    assert callable(morsel.to_arrow)

def test_take_method_signature():
    """Test that take method accepts indices and returns a Morsel."""
    table = pa.table({'a': [1, 2, 3, 4, 5], 'b': ['x', 'y', 'z', 'w', 'v']})
    morsel = draken.Morsel.from_arrow(table)
    
    # Test with list of indices
    result = morsel.take([0, 2, 4])
    assert isinstance(result, draken.Morsel)
    assert result.shape[0] == 3  # Should have 3 rows
    assert result.shape[1] == 2  # Should have same number of columns
    
    # Test with single index
    result_single = morsel.take([1])
    assert isinstance(result_single, draken.Morsel)
    assert result_single.shape[0] == 1
    
    # Test with pyarrow array
    indices_array = pa.array([0, 1], type=pa.int32())
    result_array = morsel.take(indices_array)
    assert isinstance(result_array, draken.Morsel)
    assert result_array.shape[0] == 2

def test_select_method_signature():
    """Test that select method accepts column names and returns a Morsel."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z'], 'c': [1.1, 2.2, 3.3]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Test with list of column names
    result = morsel.select(['a', 'c'])
    assert isinstance(result, draken.Morsel)
    assert result.shape[0] == 3  # Same number of rows
    assert result.shape[1] == 2  # Selected 2 columns
    
    # Test with single column name
    result_single = morsel.select(['b'])
    assert isinstance(result_single, draken.Morsel)
    assert result_single.shape[1] == 1
    
    # Test with string (single column)
    result_str = morsel.select('a')
    assert isinstance(result_str, draken.Morsel)
    assert result_str.shape[1] == 1

def test_select_nonexistent_column():
    """Test that selecting a non-existent column raises KeyError."""
    table = pa.table({'a': [1, 2, 3]})
    morsel = draken.Morsel.from_arrow(table)
    
    with pytest.raises(KeyError):
        morsel.select(['nonexistent'])

def test_rename_method_signature():
    """Test that rename method accepts names and returns a Morsel."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)
    
    # Test with list of new names
    result = morsel.rename(['col1', 'col2'])
    assert isinstance(result, draken.Morsel)
    assert result.shape == morsel.shape  # Same dimensions
    
    # Check that column names are updated
    result_arrow = result.to_arrow()
    assert result_arrow.column_names == ['col1', 'col2']
    
    # Test with dict mapping
    result_dict = morsel.rename({'a': 'alpha', 'b': 'beta'})
    assert isinstance(result_dict, draken.Morsel)
    result_dict_arrow = result_dict.to_arrow()
    assert result_dict_arrow.column_names == ['alpha', 'beta']
    
    # Test with partial dict mapping
    result_partial = morsel.rename({'a': 'alpha'})
    result_partial_arrow = result_partial.to_arrow()
    assert 'alpha' in result_partial_arrow.column_names
    assert 'b' in result_partial_arrow.column_names

def test_rename_wrong_number_names():
    """Test that providing wrong number of names raises ValueError."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)
    
    # Too few names
    with pytest.raises(ValueError):
        morsel.rename(['only_one'])
    
    # Too many names
    with pytest.raises(ValueError):
        morsel.rename(['one', 'two', 'three'])

def test_to_arrow_method():
    """Test that to_arrow method returns a pyarrow.Table."""
    table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)
    
    result = morsel.to_arrow()
    assert isinstance(result, pa.Table)
    assert result.num_columns == morsel.num_columns
    assert result.num_rows == morsel.num_rows
    
    # Column names should match (though may be decoded differently)
    original_names = set(table.column_names)
    result_names = set(result.column_names)
    assert original_names == result_names

def test_method_chaining():
    """Test that methods can be chained together."""
    table = pa.table({'a': [1, 2, 3, 4], 'b': ['w', 'x', 'y', 'z'], 'c': [1.1, 2.2, 3.3, 4.4]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Chain: select -> take -> rename
    result = (morsel
                .select(['a', 'b'])
                .take([0, 2])
                .rename(['first', 'second']))
    
    assert isinstance(result, draken.Morsel)
    assert result.shape == (2, 2)  # 2 rows, 2 columns
    
    result_arrow = result.to_arrow()
    assert result_arrow.column_names == ['first', 'second']

def test_api_compatibility_with_pyarrow():
    """Test that the method signatures are compatible with pyarrow.Table equivalents."""
    table = pa.table({'a': [1, 2, 3, 4, 5], 'b': ['v', 'w', 'x', 'y', 'z']})
    morsel = draken.Morsel.from_arrow(table)
    
    # Test indices parameter types accepted by both
    indices = [0, 2, 4]
    
    # Both should accept list of indices
    pa_result = table.take(indices)
    morsel_result = morsel.take(indices)
    assert morsel_result.shape[0] == pa_result.num_rows
    
    # Both should accept column name lists for select
    columns = ['a']
    pa_select = table.select(columns)
    morsel_select = morsel.select(columns)
    assert morsel_select.shape[1] == pa_select.num_columns
    
    # Both should accept list of names for rename
    new_names = ['col1', 'col2']
    pa_rename = table.rename_columns(new_names)
    morsel_rename = morsel.rename(new_names)
    assert morsel_rename.to_arrow().column_names == pa_rename.column_names

def test_empty_operations():
    """Test operations on empty or minimal data."""
    # Test with single row
    table = pa.table({'a': [42]})
    morsel = draken.Morsel.from_arrow(table)
    
    # Take the only row
    result = morsel.take([0])
    assert result.shape == (1, 1)
    
    # Select the only column
    result = morsel.select(['a'])
    assert result.shape == (1, 1)
    
    # Rename the only column
    result = morsel.rename(['new_name'])
    assert result.to_arrow().column_names == ['new_name']


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()