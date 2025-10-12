# Vector Consolidation Investigation

## Goal
Consolidate individual vector .pyx files into a single .so file while keeping them as separate source files, following the pattern used in Opteryx.

## Attempted Approach
Used Cython's `include` directive to create a consolidated `vector_implementations.pyx` file that includes all individual vector files, similar to how Opteryx consolidates `list_ops` and `joins` modules.

## Technical Blockers

### 1. Cython Compiler Crash
Consistent crash in Cython 3.1.3 compiler when processing included files:
```
AttributeError: 'NoneType' object has no attribute 'is_builtin_type'
```
Occurs at line 79 of `bool_vector.pyx` in `BinopNode` processing during bitshift operation `i >> 3`.

### 2. Type Resolution Issues
The compiler cannot properly resolve the type of `i` (declared as `Py_ssize_t` in method signature) when used in bitshift operations within included files.

### 3. Structural Differences from Opteryx
- **Opteryx**: Simple `cpdef` functions with basic types
- **Draken**: Complex `cdef class` hierarchies with:
  - Typed `def` method parameters (`def __getitem__(self, Py_ssize_t i)`)
  - Cython-specific pointer types (`DrakenFixedBuffer*`)
  - Complex type inference requirements

## Changes Made (Need Reverting)
1. Renamed `from_arrow` functions to unique names (`int64_from_arrow`, `bool_from_arrow`, etc.)
2. Commented out `NULL_HASH` declarations in individual vector files
3. Updated `arrow.pyx` imports to use consolidated module
4. Created auto-generation script in `setup.py`

## Recommendations

### Option 1: Accept Current Structure (Recommended)
- Keep individual .so files for each vector type
- Current structure is working, maintainable, and follows standard Cython practices
- File size overhead is minimal compared to development complexity

### Option 2: Try Different Cython Version
- Test with Cython 0.29.x which may have better `include` support
- Risk: May introduce other compatibility issues

### Option 3: Restructure Code
- Simplify vector implementations to avoid typed `def` parameters
- Use `cpdef` instead of `def` where possible
- Significant refactoring required

### Option 4: Custom Build Process
- Compile to object files, then link into single .so
- Requires complex build system modifications
- Not supported by standard setuptools/Cython workflow

## Conclusion
The Cython `include` directive approach is not feasible for draken's vector implementations with current Cython versions. Recommend reverting changes and keeping the current multi-.so structure.
