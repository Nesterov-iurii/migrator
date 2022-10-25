def main():
    import argparse
    import sqlalchemy as sa
    import re

    ms_conn = """my_connection_string"""

    my_parser = argparse.ArgumentParser(description='Helps with managing database migrations')

    my_parser.add_argument('-c', '--command', action='store', required=True)
    my_parser.add_argument('-v', '--verbose', action='store_true')
    my_parser.add_argument('-a', '--all', action='store_true')
    my_parser.add_argument('-m', '--message', action='store')
    my_parser.add_argument('--up-path', action='store')
    my_parser.add_argument('--down-path', action='store')
    my_parser.add_argument(
        '-d',
        '--dependency',
        nargs='+',
        help="migrations needed for the current in a form --dependency 1 2 3"
    )
    my_parser.add_argument('--id', action='store', type=int)

    args = my_parser.parse_args()

    command = args.command
    allowed_commands = {'list', 'ls', 'apply', 'rollback', 'add_migration', 'development_test', 'up', 'down'}


    def _setup_service_table():
        """
        creates the service table [mydatabase].[myschema].[migrations]
        returns True in all cases
        """
        ms_db = sa.create_engine(ms_conn, fast_executemany=True)
        query = """
        IF OBJECT_ID('[mydatabase].[myschema].[migrations]') IS NOT NULL
        SELECT 1 AS table_exists ELSE SELECT 0 AS table_exists;"""
        if args.verbose:
            print('Check existence of the service table...')
        result = ms_db.execute(query).fetchone()[0]
        if result == 1:
            if args.verbose:
                print('Service table exists')
            return True
        elif result == 0:
            print("The migrations service table has not been initialized. Do you want to initialize it now?")
            user_input = input('Type "y" or "yes" if you want to proceed or anything else if you want to quit ')
            if user_input in {'y', 'Y', 'yes', 'Yes', 'YES'}:
                with ms_db.begin() as conn:
                    query = """
                    CREATE TABLE [mydatabase].[myschema].[migrations] (
                        id INTEGER PRIMARY KEY IDENTITY(1,1),
                        up VARCHAR(max),
                        down VARCHAR(max),
                        comment VARCHAR(400),
                        dependency VARCHAR(400),
                        created_at DATETIME,
                        is_active TINYINT
                    );
                    """
                    conn.execute(query)
                    print('The migrations service table has been created')
                    return True
            else:
                exit()
        else:
            print('Something went wrong while checking for [mydatabase].[myschema].[migrations]')
            return False


    def _topological_sort_subroutine(G, node, seen, stack):
        seen.add(node)
        for i in G[node]:
            if i not in seen and i is not None:
                _topological_sort_subroutine(G, i, seen, stack)
        stack.insert(0, node)


    def _topological_sort(G):
        """
        return action order in stack
        """
        seen = set()
        stack = []
        for k in list(G):
            if k not in seen:
                _topological_sort_subroutine(G, k, seen, stack)
        return stack


    def _reverse_graph(G):
        from collections import defaultdict
        result = defaultdict(list)
        for key, value in G.items():
            for node in value['dependency']:
                if node is not None:
                    result[node].append(key)
        for key in G:
            if key not in result:
                result[key] = [None]
        for key, value in G.items():
            value['dependency'] = result[key]
        return G


    def _handle_list_literal(s):
        from ast import literal_eval
        if s is None:
            return [s]
        return [int(el) for el in literal_eval(s)]


    def _get_subgraph(migration_id, reverse=False):
        columns = ['id', 'dependency', 'is_active', 'up', 'down']
        ms_db = sa.create_engine(ms_conn, fast_executemany=True)
        with ms_db.begin() as conn:
            query = sa.text(f"SELECT {', '.join(columns)} FROM [mydatabase].[myschema].[migrations]")
            result = list(conn.execute(query))
        G = {id: {'dependency': _handle_list_literal(dependency), 'is_active': is_active, 'up': up, 'down': down}
                    for id, dependency, is_active, up, down in result}
        if reverse:
            G = _reverse_graph(G)
        seen = {}
        stack = [migration_id]
        node_shortlist = []
        while stack:
            cur_node = stack.pop()
            node_shortlist.append(cur_node)
            for i in G[cur_node]['dependency']:
                if i not in seen and i is not None:
                    stack.append(i)
        result = {node: G[node] for node in node_shortlist}
        return result


    def ls():
        ms_db = sa.create_engine(ms_conn)
        if args.all or args.verbose:
            query = "SELECT id, dependency, comment, created_at, is_active FROM [mydatabase].[myschema].[migrations];"
        else:
            query = """
            SELECT id, dependency, comment, created_at, is_active
            FROM [mydatabase].[myschema].[migrations]
            WHERE is_active = 1;
            """
        with ms_db.begin() as conn:
            result = list(conn.execute(query))
            G = {id: {'dependency': [dependency], 'comment': comment, 'created_at': created_at, 'is_active': is_active}
                    for id, dependency, comment, created_at, is_active in result}
        for key, value in G.items():
            print(f'{key}: {value}\n')


    def apply_migration(migration_id):
        ms_db = sa.create_engine(ms_conn, fast_executemany=True)
        if args.verbose:
            print('Getting the subgraph needed to apply the migration...')
        subgraph = _get_subgraph(migration_id)
        if args.verbose:
            print('Sorting the subgraph topologically...')
        actions = _topological_sort({key: value['dependency'] for key, value in subgraph.items()})
        actions.reverse()
        actions = [el for el in actions if subgraph[el]['is_active'] == 0]
        print(f'Here is the graph of actions to be done: {actions}')
        user_input = input('Type "y" or "yes" if you want to proceed or anything else if you want to quit ')
        if user_input not in {'y', 'Y', 'yes', 'Yes', 'YES'}:
            exit()
        for node_id in actions:
            ddl_query = subgraph[node_id]['up']
            query = f"""
            BEGIN TRANSACTION T_main;
            {ddl_query};
            UPDATE [mydatabase].[myschema].[migrations]
            SET is_active = 1 WHERE id = {node_id};
            COMMIT TRANSACTION T_main;
            """
            with ms_db.begin() as conn:
                if args.verbose:
                    print(f'Applying migration {node_id}...')
                    print(f'executing code:\n {query}')
                conn.execute(query)
                if args.verbose:
                    print('Migration successfully applied')


    def rollback_migration(migration_id):
        ms_db = sa.create_engine(ms_conn, fast_executemany=True)
        if args.verbose:
            print('Getting the subgraph needed to apply the migration...')
        subgraph = _get_subgraph(migration_id, reverse=True)
        if args.verbose:
            print('Sorting the subgraph topologically...')
        actions = _topological_sort({key: value['dependency'] for key, value in subgraph.items()})
        actions.reverse()
        actions = [el for el in actions if subgraph[el]['is_active'] == 1]
        print(f'Here is the graph of actions to be done: {actions}')
        user_input = input('Type "y" or "yes" if you want to proceed or anything else if you want to quit ')
        if user_input not in {'y', 'Y', 'yes', 'Yes', 'YES'}:
            exit()
        for node_id in actions:
            ddl_query = subgraph[node_id]['down']
            query = f"""
            BEGIN TRANSACTION T_main;
            {ddl_query};
            UPDATE [mydatabase].[myschema].[migrations]
            SET is_active = 0 WHERE id = {node_id};
            COMMIT TRANSACTION T_main;
            """
            with ms_db.begin() as conn:
                if args.verbose:
                    print(f'Rolling back migration {node_id}...')
                    print(f'executing code {query}')
                conn.execute(query)
                if args.verbose:
                    print('Migration successfully rolled back')


    def add_migration(dependency = None):
        import datetime
        write_date = datetime.datetime.today()
        write_date = write_date.strftime("%Y-%m-%d %H:%M:%S")
        ms_db = sa.create_engine(ms_conn, fast_executemany=True)
        with open(args.down_path, 'r') as content_file:
            down_code = content_file.read()
        with open(args.up_path, 'r') as content_file:
            up_code = content_file.read()
        comment = args.message
        if args.verbose:
            print('Start writing migration\'s data to database')
        if dependency is None:
            query = """
            INSERT INTO [mydatabase].[myschema].[migrations] (up, down, comment, created_at, is_active)
            VALUES (?,?,?,?,?)
            """
            with ms_db.begin() as conn:
                conn.execute(query, (up_code, down_code, comment, write_date, 0))
        else:
            query = """
            INSERT INTO [mydatabase].[myschema].[migrations] (up, down, comment, dependency, created_at, is_active)
            VALUES (?,?,?,?,?,?)
            """
            with ms_db.begin() as conn:
                conn.execute(query, (up_code, down_code, comment, str(dependency), write_date, 0))
        if args.verbose:
            print('Finish writing migration\'s data to database')


    if command not in allowed_commands:
        raise ValueError(f'The --command argument has to be in the list {allowed_commands}')
    else:
        _setup_service_table()

    if command == 'list' or command == 'ls':
        ls()

    elif command == 'apply' or command == 'up':
        if not args.id:
            raise ValueError('Need to pass the migration\'s id to to apply it')
        apply_migration(args.id)

    elif command == 'rollback' or command == 'down':
        rollback_migration(args.id)

    elif command == 'add_migration':
        if not args.up_path:
            raise ValueError('Please add the path to the up.sql file for your migration --up-path "path"')
        if not args.down_path:
            raise ValueError('Please add the path to the down.sql file for your migration --down-path "path"')
        if not args.message:
            raise ValueError('Please add the comment for you migration -m "my migration description"')
        if not args.dependency:
            user_input = input('You did not provide any dependency for this migration. Do you want to continue (type "y" or "yes")? ')
            if user_input not in {'y', 'Y', 'yes', 'Yes', 'YES'}:
                exit()
            print(f'You have entered the following dependencies for your migration: {args.dependency}')
            add_migration(dependency = None)
        else:
            print(f'You have entered the following dependencies for your migration: {args.dependency}')
            dependencies = [int(el) for el in list(args.dependency)]
            add_migration(dependency = dependencies)

    elif command == 'development_test':
        import os
        data_path = os.path.join(os.path.dirname(__file__), 'my_pkg', 'my_static.txt')
        with open(data_path, 'r') as data_file:
            print('hi')



if __name__ == "__main__":
    main()
