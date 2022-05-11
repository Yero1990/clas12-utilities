#!/usr/bin/env python2
import os
import sys
import ccdb
import argparse

class Range():
    def __init__(self, run_min, run_max, assignment):
        self.run_min = run_min
        self.run_max = run_max
        self.assignment = assignment
        self.filename = None
    @staticmethod
    def header():
        return '%6s %6s %7s  %19s  %s' % ('min','max','id','created','comment')
    def __str__(self):
        return '%6d %6d %7d  %19s  %s' % (self.run_min, self.run_max,
            self.assignment.id, self.assignment.created, self.assignment.comment)
    def table_string(self):
        rows = []
        for row in self.assignment.constant_set.data_table:
            rows.append(' '.join(row))
        return '\n'.join(rows)
    def generate_filename(self):
        path = '-'.join(self.assignment.constant_set.type_table.path.strip('/').split('/'))
        return '%s_%d-%d.txt' % (path, self.run_min, self.run_max)
    def save(self, comment=None, filename=None):
        if filename is None:
            self.filename = self.generate_filename()
        else:
            self.filename = filename
        if os.path.exists(self.filename):
            print('ERROR:  File already exists:  '+self.filename)
            print('ABORTED.')
            sys.exit()
        print('Writing file:  '+self.filename)
        with open(self.filename,'w') as f:
            if comment is not None:
                f.write('#'+comment+'\n')
            f.write(self.table_string())
    def upload_command(self):
        path = self.assignment.constant_set.type_table.path
        ret = 'ccdb -c $CCDB_CONNECTION add ' + path
        ret += ' -r %d-%d %s' % (self.run_min, self.run_max, self.filename)
        return ret

cli = argparse.ArgumentParser(description='Generate effective run ranges for a given CCDB table.',
    epilog='For example:  ccdb-ranges.py -min 4900 -max 5010 -table /runcontrol/fcup')
cli.add_argument('-min', metavar='#', help='minimum run number', type=int, required=True)
cli.add_argument('-max', metavar='#', help='maximum run number', type=int, required=True)
cli.add_argument('-table', help='table name', type=str, required=True)
cli.add_argument('-variation', help='variation name (default=default)', type=str, required=False, default='default')
cli.add_argument('-dump', help='print all constant sets', required=False, default=False, action='store_true')
cli.add_argument('-save', help='save all constant sets in files in current working directory', required=False, default=False, action='store_true')
cli.add_argument('-comment', help='add a comment to the save files', type=str, required=False)

args = cli.parse_args(sys.argv[1:])

ranges = []
provider = ccdb.AlchemyProvider()
provider.connect(os.getenv('CCDB_CONNECTION'))

for run in range(args.min, args.max+1):
    assignment = provider.get_assignment(args.table, run, args.variation)
    if len(ranges)==0 or ranges[-1].assignment.id != assignment.id:
        ranges.append( Range(run, run, assignment) )
    elif ranges[-1].run_max == run-1:
        ranges[-1].run_max = run
    else:
        raise Exception('This should be impossible.')

print('Effective Run Ranges ::::::::::::::::::::::::::::::')
print(Range.header()+'\n'+'\n'.join([str(r) for r in ranges]))
print(':::::::::::::::::::::::::::::::::::::::::::::::::::\n')

if args.dump:
    for x in ranges:
        print('\n'+str(x)+'\n'+x.table_string())
if args.save:
    for x in ranges:
        x.save(args.comment)
    for x in ranges:
        print(x.upload_command())

