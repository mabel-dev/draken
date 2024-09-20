# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False
# cython: cdivision=True

"""
Red-Black Tree with Accumulation

This module provides an implementation of a Red-Black Tree with an accumulation feature,
where multiple values can be associated with the same key. The tree maintains balanced 
insertion and allows efficient retrieval of values in key order.


Derived from https://github.com/tkluck/accumulation_tree
Licence: https://github.com/tkluck/accumulation_tree?tab=MIT-1-ov-file
"""

import operator
from typing import Any, Generator, List

cdef class Node:
    """
    Node class for Red-Black Tree.

    Attributes:
        key (object): The key of the node.
        value (list): The list of values associated with the key.
        left (Node): Left child node.
        right (Node): Right child node.
        accumulation (object): Accumulated value, purpose specific.
        red (int): Color of the node (1 for red, 0 for black).
    """
    cdef readonly object key
    cdef readonly list value
    cdef readonly Node left
    cdef readonly Node right
    cdef object accumulation
    cdef int red

    def __init__(self, key=None, value=None):
        self.key = key
        self.value = [value] if value is not None else []
        self.red = True
        self.left = None
        self.right = None

    def free(self):
        """
        Free the node's resources.
        """
        self.left = None
        self.right = None
        self.key = None
        self.value.clear()

    cdef Node get(self, int key):
        """
        Get child node by key.
        
        Parameters:
            key (int): 0 for left child, 1 for right child.
        
        Returns:
            Node: Corresponding child node.
        """
        return self.left if key == 0 else self.right

    cdef void set(self, int key, Node value):
        """
        Set child node by key.
        
        Parameters:
            key (int): 0 for left child, 1 for right child.
            value (Node): Node to set as child.
        """
        if key == 0:
            self.left = value
        else:
            self.right = value

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)


cdef int is_red(Node node):
    """
    Check if a node is red.
    
    Parameters:
        node (Node): Node to check.
    
    Returns:
        int: 1 if node is red, 0 otherwise.
    """
    if node is not None and node.red:
        return True
    return False


cdef class NullKey:
    pass


cdef NullKey null_key = NullKey()


cdef class AccumulationTree(object):
    """
    Red-Black Tree implementation.

    Attributes:
        _root (Node): Root node of the tree.
        _count (int): Number of nodes in the tree.
    """
    cdef public Node _root
    cdef public int _count

    cdef Node _new_node(self, key, value):
        """
        Create a new node.
        
        Parameters:
            key (object): Key for the new node.
            value (object): Value for the new node.
        
        Returns:
            Node: Newly created node.
        """
        self._count += 1
        return Node(key, value)

    cdef void set(self, Node node, int direction, Node child_node):
        """
        Set child node in the given direction.
        
        Parameters:
            node (Node): Parent node.
            direction (int): 0 for left, 1 for right.
            child_node (Node): Child node to set.
        """
        node.set(direction, child_node)

    def insert(self, key: Any, value: Any) -> None:
        """
        Insert a key-value pair into the tree.
        
        Parameters:
            key (Any): Key to insert.
            value (Any): Value associated with the key.
        """
        if self._root is None:  # Empty tree case
            self._root = self._new_node(key, value)
            self._root.red = False  # make root black
            return

        cdef Node head = Node(key=null_key)  # False tree root
        cdef Node grand_parent = None
        cdef Node grand_grand_parent = head
        cdef Node parent = None
        cdef int direction = 0
        cdef int last = 0

        # Set up helpers
        grand_grand_parent.right = self._root
        cdef Node node = grand_grand_parent.right

        # Search down the tree
        while True:
            if node is None:  # Insert new node at the bottom
                node = self._new_node(key, value)
                self.set(parent, direction, node)
            elif is_red(node.left) and is_red(node.right):  # Color flip
                node.red = True
                node.left.red = False
                node.right.red = False

            # Fix red violation
            if is_red(node) and is_red(parent):
                direction2 = 1 if grand_grand_parent.right is grand_parent else 0
                if node is parent.get(last):
                    self.set(grand_grand_parent, direction2, self.jsw_single(grand_parent, 1 - last))
                else:
                    self.set(grand_grand_parent, direction2, self.jsw_double(grand_parent, 1 - last))

            # Stop if found
            if key == node.key:
                node.value.append(value)  # accumulate new value for key
                break

            last = direction
            direction = 0 if key < node.key else 1
            # Update helpers
            if grand_parent is not None:
                grand_grand_parent = grand_parent
            grand_parent = parent
            parent = node
            node = node.get(direction)

        self._root = head.right  # Update root
        self._root.red = False   # make root black

    cdef Node jsw_single(self, Node root, int direction):
        """
        Perform a single rotation.
        
        Parameters:
            root (Node): Root node to rotate.
            direction (int): Direction of rotation (0 for left, 1 for right).
        
        Returns:
            Node: New root after rotation.
        """
        cdef Node save = root.get(1 - direction)
        self.set(root, 1 - direction, save.get(direction))
        self.set(save, direction, root)
        root.red = True
        save.red = False
        return save

    cdef Node jsw_double(self, Node root, int direction):
        """
        Perform a double rotation.
        
        Parameters:
            root (Node): Root node to rotate.
            direction (int): Direction of rotation (0 for left, 1 for right).
        
        Returns:
            Node: New root after rotation.
        """
        self.set(root, 1 - direction, self.jsw_single(root.get(1 - direction), 1 - direction))
        return self.jsw_single(root, direction)

    def in_order_traversal(self) -> Generator[tuple, None, None]:
        """
        Generator for in-order traversal of the tree.
        
        Yields:
            tuple: (key, list of values) in key order.
        """
        yield from self._in_order_traversal(self._root)

    def _in_order_traversal(self, Node node) -> Generator[tuple, None, None]:
        """
        Helper method for in-order traversal.
        
        Parameters:
            node (Node): Current node in the traversal.
        
        Yields:
            tuple: (key, list of values) in key order.
        """
        if node is not None:
            yield from self._in_order_traversal(node.left)
            yield (node.key, node.value)
            yield from self._in_order_traversal(node.right)

    def __getstate__(self):
        """
        Get the state of the tree for serialization.
        
        Returns:
            dict: State of the tree.
        """
        return {'payload': {k: v for k, v in self.items()}}

    def __setstate__(self, state):
        """
        Set the state of the tree from serialization.
        
        Parameters:
            state (dict): State of the tree.
        """
        self._count = 0
        self._root = None
        for k, v in state['payload'].items():
            for value in v:
                self.insert(k, value)
