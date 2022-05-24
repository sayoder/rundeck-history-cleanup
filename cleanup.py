#!/usr/bin/python3
import os
import pymysql
from argparse import ArgumentParser
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()

HARD_MAX = 1000

DB_CONN = {
    'user': os.getenv('DB_USER'),
    'host': os.getenv('DB_HOST'),
    'port': 3306,
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME')
}

def get_rows(max_execs, months, project):
    cnx = pymysql.connect(**DB_CONN)

    try:
        cursor = cnx.cursor()

        sql='''
        SELECT
            e.id AS execution_id,
            lfsr.id AS log_file_storage_request_id,
            br.id AS base_report_id,
            o.id AS orchestrator_id,
            w.id AS workflow_id,
            ws.id AS workflow_step_id
        FROM

        ( SELECT id, workflow_id, orchestrator_id
            FROM execution
            WHERE date_started < DATE_SUB(NOW(), INTERVAL %(months)s MONTH)
        '''
        sql += ( "AND project = %(project)s " if project else '' )
        sql += '''
        LIMIT %(max_execs)s ) e

        JOIN workflow w ON e.workflow_id = w.id
        LEFT JOIN log_file_storage_request lfsr ON lfsr.execution_id = e.id
        LEFT JOIN base_report br ON br.jc_exec_id = e.id
        JOIN orchestrator o ON e.orchestrator_id = o.id
        JOIN workflow_workflow_step wws ON wws.workflow_commands_id = w.id
        JOIN workflow_step ws ON wws.workflow_step_id = ws.id;
        '''

        # print(sql)

        params = {
            'max_execs' : max_execs,
            'months' : months,
        }

        if project:
            params['project'] = project

        cursor.execute(sql, params)
        rows = cursor.fetchall()

    finally:
        cnx.close()

    column_headers = tuple(h[0] for h in cursor.description)
    rows = list(zip(*rows)) # Convert to vertical slices

    '''
    Dict format:
    {
        'br_id': [130697879],
        'e_id': [114668209, 114672782, 114674331, 114674892, 114674937],
        'lfsr_id': [],
        'o_id': [7894, 12467, 14016, 14577, 14622],
        'w_id': [114668779, 114673352, 114674901, 114675462, 114675507],
        'ws_id': [115201829, 115206437, 115208002, 115208567, 115208615]
    }
    '''
    row_dict = {}

    for i in range(len(rows)):
        id_list = list(filter(lambda v: v is not None, rows[i]))
        row_dict[column_headers[i]] = list(set(id_list))

    # pprint(row_dict)
    return row_dict

def delete_rows(row_dict, dry_run):
    cnx = pymysql.connect(**DB_CONN)
    cursor = cnx.cursor()

    try:
        # Order matters here; we need to respect foreign key constraints.
        for key in ['base_report_id',
                    'log_file_storage_request_id',
                    'execution_id',
                    'orchestrator_id',
                    'workflow_step_id',
                    'workflow_id']:
            if row_dict[key]:
                table = key[:-3]

                idlist = ','.join([str(val) for val in row_dict[key]])

                if table in ('workflow', 'workflow_step'):
                    workflow_tbl_field_map = { 'workflow' : 'workflow_commands_id',
                                               'workflow_step' : 'workflow_step_id'
                                             }
                    sql = ( '''DELETE FROM workflow_workflow_step WHERE '''
                        + workflow_tbl_field_map[table]
                        + ''' IN (%s)''' % idlist
                    )
                    print(sql)

                    if not dry_run:
                        cursor.execute(sql)
                        cnx.commit()

                sql = '''DELETE FROM ''' + table + ''' WHERE id IN (%s)''' % idlist
                print(sql)
                if not dry_run:
                    cursor.execute(sql)
                    cnx.commit()

    except:
        print(cursor._last_executed)
        raise

    finally:
        cnx.close()


def main(dry_run, max_execs, months, project):
    if max_execs > HARD_MAX:
        max_execs = HARD_MAX

    row_dict = get_rows(max_execs, months, project)
    delete_rows(row_dict, dry_run)
    print()


if __name__ == '__main__':
    parser = ArgumentParser(description="""
        Tool for deleting execution data from Rundeck databases.
    """)

    parser.add_argument('--dry-run', default=False, action='store_true', help="Don't run any DELETEs")
    parser.add_argument('--max', required=True, type=int, help="Maximum number of executions to delete")
    parser.add_argument('--months', required=True, type=int, help="Delete executions older than this many months")
    parser.add_argument('--project', required=False, type=str, default='', help="Project to delete executions from")

    args = parser.parse_args()

    main(args.dry_run, args.max, args.months, args.project)

