import random
from typing import Any
from typing import Optional
from typing import Tuple


class SkipNode:
    def __init__(self, key: Any, value: Any, level: int):
        self.key = key
        self.value = value
        self.forward = [None] * (level + 1)


class SkipList:
    def __init__(self, max_level: int):
        self.max_level = max_level
        self.head = SkipNode(None, None, max_level)
        self.level = 0

    def random_level(self) -> int:
        level = 0
        while random.random() < 0.5 and level < self.max_level:
            level += 1
        return level

    def search(self, key: Any) -> Optional[Tuple[Any, Any]]:
        current = self.head
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
        current = current.forward[0]
        if current and current.key == key:
            return current.key, current.value
        return None

    def insert(self, key: Any, value: Any) -> None:
        update = [None] * (self.max_level + 1)
        current = self.head

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        level = self.random_level()
        if level > self.level:
            for i in range(self.level + 1, level + 1):
                update[i] = self.head
            self.level = level

        new_node = SkipNode(key, value, level)
        for i in range(level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

    def delete(self, key: Any) -> None:
        update = [None] * (self.max_level + 1)
        current = self.head

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]
        if current and current.key == key:
            for i in range(self.level + 1):
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]

            while self.level > 0 and not self.head.forward[self.level]:
                self.level -= 1


# Example usage
skip_list = SkipList(3)
skip_list.insert(3, "three")
skip_list.insert(6, "six")
skip_list.insert(7, "seven")
skip_list.insert(9, "nine")
skip_list.insert(12, "twelve")
skip_list.insert(19, "nineteen")

print(skip_list.search(6))  # Output: (6, 'six')
print(skip_list.search(15))  # Output: None

skip_list.delete(6)
print(skip_list.search(6))  # Output: None
