# Draken

External Indexes for Opteryx.

## Index Types

Binary Search Index

## Index Structure

Based on an SSTable:

Header
Schema Block
Bloom Filter
Key Block
Data Block

As the data being indexed is immutable, structures with features which support updates (such as B+Trees) are not required.

This structure allows for point searches and range searches.