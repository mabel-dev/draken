# DRAKEN

DRAKEN is a columnar vector library written in Cython/C. It provides Arrow-compatible memory layouts with type-specialized vectors and fast kernels for core operations.

We’re building it because PyArrow, while excellent for data interchange, is too general-purpose and adds overhead in the hot loops of a SQL execution engine. DRAKEN strips that away to give us leaner buffers, predictable layouts, and tighter control over performance-critical kernels.

DRAKEN will be used inside Opteryx as the internal container format, replacing PyArrow in execution paths while still interoperating with Arrow for I/O.

The expected benefits are higher speed, lower overhead, and more control over memory and null handling. What makes DRAKEN unique is its narrow focus: it isn’t a dataframe library like Polars or DuckDB, and it isn’t a general API like PyArrow — it’s a purpose-built execution container designed specifically for python database kernels.
