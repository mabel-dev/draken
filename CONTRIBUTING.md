# Contributing to DRAKEN

Thank you for your interest in contributing to DRAKEN! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

- Python 3.11+
- Cython 3.1.3+
- C++17 compatible compiler
- Git

### Setting up your development environment

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/draken.git
   cd draken
   ```

3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

4. Compile the Cython extensions:
   ```bash
   make compile
   ```

5. Run the tests to verify everything works:
   ```bash
   make test
   ```

## Development Workflow

### Making Changes

1. Create a new branch for your feature/fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and ensure they follow our coding standards:
   ```bash
   make lint
   ```

3. Add or update tests for your changes

4. Run the test suite:
   ```bash
   make test
   make coverage
   ```

5. Commit your changes with a clear commit message

### Code Style

We use several tools to maintain code quality:

- **Black** for Python code formatting
- **isort** for import sorting  
- **Ruff** for linting
- **cython-lint** for Cython code

Run all linting tools with:
```bash
make lint
```

### Testing

- Write tests for any new functionality
- Ensure existing tests continue to pass
- Aim for high test coverage on new code
- Performance tests should demonstrate improvement over PyArrow where applicable

### Pull Requests

1. Push your branch to your fork on GitHub
2. Create a pull request from your branch to the main repository
3. Ensure your PR description clearly describes the changes
4. Link to any relevant issues
5. Be responsive to code review feedback

## Types of Contributions

### Bug Reports

When reporting bugs, please include:
- Python version and platform
- DRAKEN version
- Minimal code to reproduce the issue
- Expected vs actual behavior
- Stack trace if applicable

### Feature Requests

For new features:
- Explain the use case
- Consider performance implications
- Discuss Arrow compatibility if relevant
- Provide example usage

### Performance Improvements

- Include benchmarks demonstrating improvement
- Consider memory usage as well as speed
- Test on multiple platforms if possible
- Document any trade-offs

### Documentation

- Keep documentation up to date with code changes
- Add examples for new features
- Improve clarity and correctness

## Project Structure

```
draken/
├── draken/
│   ├── core/           # Core buffer and type definitions
│   ├── vectors/        # Type-specialized vector implementations
│   ├── morsels/        # Batch processing containers  
│   └── interop/        # Arrow interoperability layer
├── tests/              # Test suite
│   ├── vectors/        # Vector-specific tests
│   └── performance/    # Performance benchmarks
├── docs/               # Documentation
└── Makefile           # Development commands
```

## Getting Help

- Check existing issues and discussions on GitHub
- Ask questions in new issues with the "question" label
- Reach out to maintainers for guidance on larger contributions

## License

By contributing to DRAKEN, you agree that your contributions will be licensed under the Apache License 2.0.