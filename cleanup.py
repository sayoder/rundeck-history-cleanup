#!/usr/bin/python3
import os
import pymysql
from argparse import ArgumentParser
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()

HARD_MAX = 1000000

DB_CONN = {
    'user': os.getenv('DB_USER'),
    'host': os.getenv('DB_HOST'),
    'port': 3306,
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME')
}

def get_rows(max_execs, days, project):
    cnx = pymysql.connect(**DB_CONN)
    try:
        cursor = cnx.cursor()

        project_filter = "AND project = %(project)s" if project else ''
        subq = f'''\
        SELECT id, workflow_id, orchestrator_id
        FROM   execution
        WHERE  date_started < DATE_SUB(NOW(), INTERVAL %(days)s DAY)
        {project_filter}
        LIMIT  %(max_execs)s
        '''

        # Select order matters here -- we will need to respect foreign key
        # constraints when we delete rows.
        sql=f'''\
        SELECT
            br.id AS base_report_id,
            lfsr.id AS log_file_storage_request_id,
            e.id AS execution_id,
            o.id AS orchestrator_id,
            ws.id AS workflow_step_id,
            w.id AS workflow_id
        FROM ({subq}) e
        LEFT JOIN      workflow w ON e.workflow_id = w.id
        LEFT JOIN log_file_storage_request lfsr ON lfsr.execution_id = e.id
        LEFT JOIN base_report br ON br.jc_exec_id = e.id
        LEFT JOIN      orchestrator o ON e.orchestrator_id = o.id
        LEFT JOIN      workflow_workflow_step wws ON wws.workflow_commands_id = w.id
        LEFT JOIN      workflow_step ws ON wws.workflow_step_id = ws.id;
        '''
        params = {
            'max_execs' : max_execs,
            'days' : days,
        }
        if project:
            params['project'] = project
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    finally:
        cnx.close()

    '''
    Dict format:
    {
        'base_report_id': [124009226, 124008862],
        'log_file_storage_request_id': [123952872, 123952873, 123952509],
        'execution_id': [123990802, 123990804, 123990399],
        'orchestrator_id': [9329616, 9329618, 9329213],
        'workflow_step_id': [124601434, 124601028, 124601436],
        'workflow_id': [123991384, 123990979, 123991382]
    }
    '''
    row_dict = {}
    column_headers = tuple(h[0] for h in cursor.description)

    # Convert to vertical slices -- we want lists of IDs that reference the
    # same table, not a record set containing one of each type of ID.
    rows = list(zip(*rows))

    for i in range(len(rows)):
        # Remove NULL values and duplicates
        id_list = [v for v in rows[i] if v is not None]
        row_dict[column_headers[i]] = list(set(id_list))
    return row_dict

def delete_rows(row_dict, dry_run, verbose):
    cnx = pymysql.connect(**DB_CONN)
    cursor = cnx.cursor()

    try:
        for id_field in row_dict.keys():
            if not row_dict[id_field]:
                raise Exception('Coding error -- invalid ID field')

            # Remove _id from each field name to get our table name
            table = id_field[:-3]
            idlist = ','.join([str(v) for v in row_dict[id_field]])

            # Remove rows from workflow_workflow_step (many-to-many table)
            if table in ('workflow', 'workflow_step'):
                workflow_tbl_field_map = {
                    'workflow' : 'workflow_commands_id',
                    'workflow_step' : 'workflow_step_id'
                }
                sql = (
                    '''DELETE FROM workflow_workflow_step WHERE '''
                    + workflow_tbl_field_map[table]
                    + ''' IN (%s)''' % idlist
                )
                if verbose:
                    print(sql)
                if not dry_run:
                    cursor.execute(sql)

            sql = '''DELETE FROM ''' + table + ''' WHERE id IN (%s)''' % idlist
            if verbose:
                print(sql)
            if not dry_run:
                cursor.execute(sql)
        cnx.commit()
    except:
        print(cursor._last_executed)
        raise
    finally:
        cnx.close()


def main(dry_run, max_execs, days, project, verbose):
    if max_execs > HARD_MAX:
        max_execs = HARD_MAX

    row_dict = get_rows(max_execs, days, project)
    delete_rows(row_dict, dry_run, verbose)


if __name__ == '__main__':
    parser = ArgumentParser(description="""
        Tool for deleting execution data from Rundeck databases.
    """)

    parser.add_argument('--dry-run', default=False, action='store_true', help="Don't run any DELETEs")
    parser.add_argument('--max', required=True, type=int, help="Maximum number of executions to delete")
    parser.add_argument('--days', required=True, type=int, help="Delete executions older than this many days")
    parser.add_argument('--project', required=False, type=str, default='', help="Project to delete executions from")
    parser.add_argument('--verbose', required=False, default=False, action='store_true', help="Log DELETE queries")

    args = parser.parse_args()

    main(args.dry_run, args.max, args.days, args.project, args.verbose)

