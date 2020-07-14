# -*- coding: utf-8 -*-

from ansible.module_utils.basic import AnsibleModule


import datetime
try:
    import sqlalchemy
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

DOCUMENTATION = '''
module: sql_query
short_description: Select/Insert/Update/Delete records in an sql database
description: >
               This module uses sqlalchemy to query different types of sql databases
               with the given table structure, keys and values.
requirements:
  - python-sqlalchemy
options:
  name:
    description: "Database connection URL: https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls"
    required: true
    aliases:
      - url
      - db
  table:
    description: Name of the table
    required: true
  keys:
    description: >
                   List of table columns that should be used for the where clause
    required: true
  columns:
    description: >
                   List of columns with name, type and value.
                   If the name is in keys, the value is used in the where clause, else for Update.
                   All columns are used if the value is to be inserted.
                   No value will be changed if state is select or count. type can be one of: String, Integer,
                   BigInteger, Boolean, Date, DateTime, Text. value for Date or DateTime can be "now",
                   and will be replaced by the current date/datetime.
    required: false
  state:
    description: >
                   present = insert/update, absent = delete,
                   select = fetch columns, insert = always insert,
                   count = select count
    required: false
    default: present
  filter:
    description: >
                  Advanced filter for select and count.
                  All column operators from sqlalchemy should work.
    required: false
  distinct:
    description: >
                   Enables SELECT DISTINCT
    required: false
    default: no
'''

EXAMPLES = '''
- name: "Ensure col2 is set to 1 and col3 is set to 2020-05-01 12:15:00 where col1 = 'blubb'"
  sql_query:
    name: "mysql://root:somepw@localhost/test"
    table: test
    keys:
    - col1
    columns:
    - name: col1
      type: String
      value: blubb
    - name: col2
      type: Integer
      value: 1
    - name: col3
      type: DateTime
      value: "2020-05-01 12:15:00"

- name: "Add Log entry to table somelog"
  sql_query:
    name: "mysql://root:somepw@localhost/test"
    table: somelog
    columns:
    - name: logtext
      type: String
      value: some important text
    - name: logtime
      type: DateTime
      value: now
    state: insert

- name: "select col1, col2 from test where col1 = 'blubb'"
  sql_query:
    name: "mysql://localhost/test?read_default_file=/root/.my.cnf"
    table: test
    keys:
    - col1
    columns:
    - name: col1
      type: String
      value: blubb
    - name: col2
      type: Integer
    state: select

- name: "delete from test where col1 = 'blubb'"
  sql_query:
    name: "sqlite:////absolute/path/to/foo.db"
    table: test
    keys:
    - col1
    columns:
    - name: col1
      type: String
      value: blubb
    state: absent

- name: "Ensure col3 is set to dadada where col1 = 'blubb' and col2=1"
  sql_query:
    name: "mysql://localhost/test?read_default_file=/root/.my.cnf"
    table: test2
    keys:
    - col1
    - col2
    columns:
    - name: col1
      type: String
      value: blubb
    - name: col2
      type: Integer
      value: 1
    - name: col3
      type: String
      value: dadada

- name: "Ensure col2 is set to 1 where col1 = 'blubb'"
  sql_query:
    name: "mysql://localhost/test?read_default_file=/root/.my.cnf"
    table: test
    keys:
    - col1
    columns:
    - name: col1
      type: String
      value: blubb
    - name: col2
      type: Integer
      value: 1

- name: This is an advanced select example using sqlalchemy filters
  sql_query:
    name: "mysql://user:pw@127.0.0.1:3306/somedb"
    table: "some_table_name"
    keys:
    - col1
    filter:
      and:
        or:
          eq:
            column: col1
            value: blubb
          eq:
            column: col1
            value: bla
        ne:
          column: col2
          value: 21
        ilike:
          column: col1
          value: test%
    columns:
    - name: col1
      type: String
    - name: col2
      type: Integer
    state: select
'''

RETURN = r'''
rows:
  description: A list of rows that have been set, deleted or selected.
  returned: always
  type: list
  sample: "[{'col1': 'val1', 'col2': 2}]"
changed:
  description: If an INSERT,UPDATE or DELETE query was executed (or check mode).
  returned: always
  type: bool
  sample: "True"
'''


def to_datetime(x, date=False):
    if not isinstance(x, datetime.datetime) and not isinstance(x, datetime.date):
        if x.strip() == 'now':
            x = datetime.datetime.now()
        else:
            if date:
                x = datetime.datetime.strptime(x, '%Y-%m-%d')
            else:
                x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    if date and isinstance(x, datetime.datetime):
        return x.date()
    return x


TYPE_FOR_NAME = {
    'String': {
        'sqlalchemy': sqlalchemy.String,
        'python': lambda x: str(x),
    },
    'Integer': {
        'sqlalchemy': sqlalchemy.Integer,
        'python': lambda x: int(x),
    },
    'BigInteger': {
        'sqlalchemy': sqlalchemy.BigInteger,
        'python': lambda x: int(x),
    },
    'Boolean': {
        'sqlalchemy': sqlalchemy.Boolean,
        'python': lambda x: bool(x),
    },
    'Date': {
        'sqlalchemy': sqlalchemy.Date,
        'python': lambda x: to_datetime(x, True),
    },
    'DateTime': {
        'sqlalchemy': sqlalchemy.DateTime,
        'python': lambda x: to_datetime(x, False),
    },
    'Text': {
        'sqlalchemy': sqlalchemy.Text,
        'python': lambda x: str(x),
    },
}


class SQLQuery(object):

    def __init__(self, module):
        self.module = module
        self.init_sqlalchemy()
        if not self.table_exists(self.module.params.get('table')):
            self.module.fail_json(msg='table does not exist')
        self.create_table()
        rows_after = self.select_rows()
        rows_after_len = len(rows_after)
        if self.module.params.get('state') == 'select' or self.module.params.get('state') == 'count':
            self.module.exit_json(changed=False, rows=self.format_rows(rows_after))
        elif self.module.params.get('state') == 'absent':
            if rows_after_len < 1:
                changed = False
            else:
                changed = True
            if changed and not self.module.check_mode:
                self.delete_rows()
            self.module.exit_json(changed=changed, rows=None)
        elif self.module.params.get('state') == 'insert':
            changed = True
            if not self.module.check_mode:
                self.insert_row()
            rows_after = self.select_rows()
            self.module.exit_json(changed=changed, rows=self.format_rows(rows_after))
        else:
            if rows_after_len < 1:
                changed = True
            else:
                changed = self.compare_rows(rows_after)
            if self.module.check_mode:
                rows_after = self.new_values
            elif changed:
                if rows_after_len < 1:
                    self.insert_row()
                else:
                    self.update_rows()
                rows_after = self.select_rows()
            self.module.exit_json(changed=changed, rows=self.format_rows(rows_after))

    def format_rows(self, rows):
        frows = []
        for row in rows:
            frow = {}
            for col in row.keys():
                type_func = self.type_for_column[col]['python']
                value = row[col]
                if value is not None:
                    frow[col] = type_func(value)
                else:
                    frow[col] = value
            frows.append(frow)
        return frows

    def compare_rows(self, rows):
        for row in rows:
            for col in self.new_values:
                if self.new_values[col] != row[col]:
                    return True
        return False

    def select_rows(self):
        stmt = self.table.select()
        stmt = self.where_keys(stmt)
        if self.module.params.get('distinct'):
            stmt = stmt.distinct()
        if self.module.params.get('state') == 'count':
            stmt = stmt.count()
        result = stmt.execute()
        return [row for row in result]

    def insert_row(self):
        stmt = self.table.insert(None)
        stmt = stmt.values(**self.new_values)
        stmt.execute()

    def update_rows(self):
        stmt = self.table.update(None)
        stmt = self.where_keys(stmt)
        args = {}
        for col in self.new_values:
            if col not in self.keys:
                args[col] = self.new_values[col]
        stmt = stmt.values(**args)
        stmt.execute()

    def delete_rows(self):
        stmt = self.table.delete(None)
        stmt = self.where_keys(stmt)
        stmt.execute()

    def init_sqlalchemy(self):
        self.engine = sqlalchemy.create_engine(
            name_or_url=self.module.params.get('name'),
            isolation_level='READ UNCOMMITTED')
        self.metadata = sqlalchemy.MetaData()
        self.metadata.bind = self.engine

    def create_table(self):
        self.keys = self.module.params.get('keys')
        self.new_values = {}
        self.type_for_column = {}

        columns = []
        for arg in self.module.params.get('columns'):
            column_name = arg['name']
            type_name = arg['type']
            try:
                value = arg['value']
                value_exists = True
            except KeyError:
                value = None
                value_exists = False
            args = {}
            columns.append(sqlalchemy.Column(column_name,
                           TYPE_FOR_NAME[type_name]['sqlalchemy'],
                           **args))
            if value is not None:
                value = TYPE_FOR_NAME[type_name]['python'](value)
            if value_exists:
                self.new_values[column_name] = value
            self.type_for_column[column_name] = \
                TYPE_FOR_NAME[type_name]
        self.table = sqlalchemy.Table(self.module.params.get('table'),
                                      self.metadata,
                                      *columns)

    def where_keys(self, stmt):
        args = []
        for col in self.keys:
            new_value = None
            try:
                new_value = self.new_values[col]
            except KeyError:
                pass
            else:
                args.append(self._where_column_helper(col) == new_value)

        fltrs = self.module.params.get('filter')
        if fltrs:
            self._split_filter(args, fltrs)

        if len(args) > 0:
            stmt = stmt.where(sqlalchemy.sql.and_(*args))
        return stmt

    def _where_column_helper(self, column_name):
        try:
            return getattr(self.table.c, column_name)
        except (AttributeError, KeyError) as e:
            _ = e
            pass
        raise ValueError('Column {} does not exist'.format(column_name))

    def _split_filter(self, args, fltrs):
        for key in fltrs:
            if key == 'and':
                sub_args = []
                self._split_filter(sub_args, fltrs[key])
                args.append(sqlalchemy.sql.and_(*sub_args))
            elif key == 'or':
                sub_args = []
                self._split_filter(sub_args, fltrs[key])
                args.append(sqlalchemy.sql.or_(*sub_args))
            else:
                column = self._where_column_helper(fltrs[key]['column'])
                column_value = fltrs[key]['value']
                if key == 'eq':
                    args.append(column == column_value)
                elif key == 'ne':
                    args.append(column)
                else:
                    try:
                        action = getattr(column, key)
                    except (AttributeError, KeyError):
                        raise ValueError('Unknown operator on column {}'.format(key))
                    args.append(action(column_value))

    def table_exists(self, name):
        ret = self.engine.dialect.has_table(self.engine, name)
        return ret


def main():
    module = AnsibleModule(
        argument_spec=dict(
            name=dict(required=True, aliases=['db', 'url']),
            table=dict(required=True, aliases=['tb']),
            keys=dict(required=False, aliases=['pk'], type='list', default=[]),
            columns=dict(required=True, type='list'),
            state=dict(default='present',
                       choices=['present', 'absent', 'select', 'insert', 'count']),
            distinct=dict(default=False, type='bool'),
            filter=dict(required=False, default={}, type='dict')
        ),
        supports_check_mode=True)
    if not HAS_SQLALCHEMY:
        module.fail_json(msg='python-sqlalchemy not found')

    SQLQuery(module)


if __name__ == '__main__':
    main()
