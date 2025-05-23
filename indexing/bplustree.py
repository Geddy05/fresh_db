import math

class Node:

    def __init(self, order) -> None:
        self.order = order
        self.values = []
        self.keys = []
        self.nextKey = None
        self.parent = None
        self.check_leaf = False

        # Search operation
    def search(self, value):
        current_node = self
        while(not current_node.check_leaf):
            temp2 = current_node.values
            for i in range(len(temp2)):
                if (value == temp2[i]):
                    current_node = current_node.keys[i + 1]
                    break
                elif (value < temp2[i]):
                    current_node = current_node.keys[i]
                    break
                elif (i + 1 == len(current_node.values)):
                    current_node = current_node.keys[i + 1]
                    break
        return current_node


class BplusTree:
    def __init__(self, order) -> None:
        self.root = Node(order)
        self.root.check_leaf = True

    def search(self, value):
        current_node = self.root
        while (not current_node.check_leaf):
            temp2 = current_node.values
            for i in range(len(temp2)):
                if (value == temp2[i]):
                    current_node = current_node.keys[i + 1]
                    break
                elif( value < temp2[i]):
                    current_node = current_node.keys[i]
                    break
                elif (i+1 == len(current_node.values)):
                    current_node = current_node.keys[i + 1]
                    break
        return current_node
                

    