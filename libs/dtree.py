from enum import IntEnum
from collections import deque

const = IntEnum('Constants', 'END START', start=-1)


class Base:
    def __init__(self, data=None, **kwargs):
        self.type = None
        if data:
            self.id = data.get('id')
            self.name = data.get('name')
            self.parent = data.get('parent')
            self.columns = data.get('columns', [])
        else:
            self.id = kwargs.get('id')
            self.name = kwargs.get('name')
            self.parent = kwargs.get('parent')
            self.columns = kwargs.get('columns', [])

    @property
    def tree(self):
        item = self
        while not isinstance(item, DTree):
            item = item.parent
            if not item:
                break
        return item

    def clone(self, dst):
        if isinstance(self, Node):
            node = Node(name=self.name)
            items = [node]
            results = []
            if isinstance(dst, Node):
                dst.append(node)
                results = node.populate(self.to_list())

            return items + results

    def get(self, columns=None):
        if not columns:
            columns = (0, )
        elif isinstance(columns, int):
            columns = (columns, )
        elif not isinstance(columns, tuple):
            return

        data = []
        for column in columns:
            if column < 0 or column > len(self.columns):
                continue
            elif not column:
                data.append(self.name)
            elif 0 < column < len(self.columns)+1:
                data.append(self.columns[column-1])

        return data[0] if len(data) == 1 else tuple(data) if data else None

    def set(self, columns, values):
        if not isinstance(columns, tuple) or not isinstance(values, tuple):
            values = (values, )

        if isinstance(columns, int):
            columns = (columns, )
        elif not isinstance(columns, tuple):
            return

        for column, value in dict(zip(columns, values)).items():
            if column < 0 or column > len(self.columns):
                continue
            elif not column:
                self.name = value
            else:
                self.columns[column-1] = value

    def path(self):
        uri = []
        item = self
        while item is not None:
            uri.append(item.name)
            item = item.parent
        return '/'.join(list(reversed(uri))).lstrip('.')

    def delete(self, item=None):
        node = item if item else self
        parent = node.parent
        del parent[parent.index(node)]

    def is_node(self, item=None):
        if item is None:
            item = self
        return True if isinstance(item, Node) else False


class Leaf(Base):
    def __init__(self, data=None,  **kwargs):
        super().__init__(data, **kwargs)
        self.type = 'Leaf'
        if self.parent is not None:
            self.parent.append(self)

    def __len__(self):
        return None


class Node(Base, deque):
    def __init__(self, data=None, **kwargs):
        Base.__init__(self, data, **kwargs)
        deque.__init__(self)
        self.type = 'Node'
        if self.parent is not None:
            self.parent.append(self)

    @property
    def children(self):
        return self

    def move(self, dst):
        if isinstance(dst, Node):
            node = Node(name=self.name)
            items = [node]
            dst.append(node)
            items += node.populate(self.to_list())
            self.delete()
            return items

    def show(self, **kwargs):
        indent = kwargs.get('indent', 2)
        parent = kwargs.get('parent', self)
        show_id = kwargs.get('show_id', False)
        show_columns = kwargs.get('show_columns', True)

        def walk(_parent, level=0):
            if not _parent.is_node():
                return _parent

            for idx, _node in enumerate(_parent):
                pad = '' if not level else '─' * (indent * level)
                for i, c in enumerate(_node.columns):
                    if c is None:
                        _node.columns[i] = ''

                columns = str(_node.columns[0]) if len(_node.columns) == 1 else str(_node.columns)
                columns = '' if not _node.columns or not show_columns else f': {columns}'

                end = '>' if _node.type == 'Node' else '─'
                node_id = f' {_node.id},' if show_id else ''
                print(f' ├{pad}{end}{node_id} {_node.name}{columns}')
                if _parent.is_node(_node):
                    walk(_node, level+1)

        print('-----------------------------------------------------')
        print(f'Name: {self.name}, Zones: ')
        print('-----------------------------------------------------')
        walk(parent)

        return self

    def query(self, query):
        if isinstance(query, int):
            item = self.find_by_id(query)
        elif isinstance(query, str):
            item = self.find(query)
        else:
            item = None

        return item

    def append(self, item, parent=None) -> str:
        parent = parent if parent else self

        new_item = ''
        if self.tree.unique:
            for child in parent:
                if child.name == item.name and self.tree.errors != 'ignore':
                    message = f'duplicate name {item.path()} found.'
                    raise ValueError(message)

        if isinstance(item, Leaf) or isinstance(item, Node):
            super(Node, self).append(item)

            new_item = parent[len(parent)-1]
            if new_item.parent is None:
                new_item.parent = parent

            item.id = self.tree.next_id()
            item.columns += [None] * (len(self.tree.data_columns) - len(item.columns))

        return new_item

    def insert(self, idx, item, parent=None):
        parent = parent if parent else self

        if self.tree.unique:
            for child in parent:
                if child.name == item.name and self.tree.errors != 'ignore':
                    message = f'duplicate name {item.path()} found.'
                    raise ValueError(message)

        if idx == int(const.END):
            idx = len(parent)
        elif idx < int(const.START):
            idx = int(const.START)

        if isinstance(item, Leaf) or isinstance(item, Node):
            super(Node, self).insert(idx, item)

            if item.parent is None:
                item.parent = parent

            item.id = self.tree.next_id()
            item.columns += [None] * (len(self.tree.data_columns) - len(item.columns))
        return item

    def to_list(self, parent=None):
        def set_data(_item, _data):
            for node in _item:
                _item_data = {'name': node.name, 'columns': node.columns}
                _data.append(_item_data)
                if node.is_node():
                    _item_data['children'] = []
                    set_data(node, _item_data['children'])

        data = []
        parent = parent if parent else self
        for item in parent:
            if item.is_node():
                item_data = {'name': item.name, 'columns': item.columns, 'children': []}
                data.append(item_data)
                set_data(item, item_data['children'])
            else:
                item_data = {'name': item.name, 'columns': item.columns}
                data.append(item_data)
        return data

    def populate(self, data, **kwargs):
        def walk(parent, item):
            if 'children' in item:
                new_node = Node(**item)
                parent.append(new_node, parent=parent)
                if 'children' in item and len(item['children']):
                    for node in item['children']:
                        walk(new_node, node)
            else:
                new_node = Leaf(**item)
                parent.append(new_node, parent=parent)

            items.append(new_node)

        if not data:
            return

        items = []
        if isinstance(data, list):
            for cfg in data:
                walk(kwargs.get('parent', self), cfg)

        return items

    def get_cell(self, row, column):
        item = self.query(row)
        if item is None:
            return
        elif column:
            return item.get(column)
        else:
            return self.name

    def set_cell(self, row, column, value):
        item = self.query(row)
        if item is None:
            return
        elif column:
            item.set(column, value)
        else:
            item.name = value

    def find_all(self, query, recursive=False):
        def find(item):
            if item.name == query:
                items.append(item)

            for child in item:
                if '/' in query and query not in child.path() and child.is_node():
                    for d in child:
                        if query in d.path() and d not in items:
                            items.append(d)
                        if d.is_node() and recursive:
                            find(d)
                elif query in child.path() and child not in items:
                    items.append(child)
                elif recursive and child.is_node():
                    find(child)

        items = []
        find(self)
        return items

    def find_by_id(self, _id):
        for child in self:
            if child.id == _id:
                return child

            if child.is_node() and len(child):
                result = child.query(_id)
                if result is not None:
                    return result

    def find(self, query, **kwargs):
        def search(parent, _query):
            for _child in parent:
                if _child.name == _query:
                    return _child

            for _child in parent:
                if _child.is_node():
                    result = search(_child, _query)
                    if result is not None:
                        return result

        _all = kwargs.get('all', False)

        if _all:
            return self.find_all(query, recursive=kwargs.get('recursive', True))

        if '/' in query:
            if query.startswith('/'):
                item = None
                parts = query.lstrip('/').split('/', 1)
                for child in self.tree:
                    if child.name == parts[0]:
                        item = child
                        break
            else:
                parts = query.split('/', 1)
                item = search(self, parts.pop(0))

            if item and query in item.path():
                return item
            elif item:
                return item.find(parts[0])
        else:
            return search(self, query)


class DTree(Node):
    def __init__(self, **kwargs):
        self.id = 0
        self.items = 0

        self.type = 'Tree'
        self.errors = kwargs.get('errors')
        self.unique = kwargs.get('unique', True)
        self.data_columns = kwargs.get('data_columns', [])
        super().__init__()

        self.name = kwargs.get('name', '.')
        self.parent = kwargs.get('parent')

        if self.parent is not None:
            self.parent.append(self)

        self.label = kwargs.get('label', '')

    def next_id(self):
        self.items += 1
        return self.items

    def reindex(self, start=0):
        def walk(_parent):
            for _item in _parent:
                _item.id = self.next_id()
                if _item.is_node():
                    walk(_item)

        self.items = start
        for item in self:
            item.id = self.next_id()
            if item.is_node():
                walk(item)
