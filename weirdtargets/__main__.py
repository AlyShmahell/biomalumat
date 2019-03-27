from weirdtargets import *

def smalltargets():
    args = SmallTargetsArgParse()
    small_targets = SmallTargets(args['type'], args['id'])
    small_targets.testParallelism()
    small_targets()
    print(small_targets)

def bigtargets():
    args = BigTargetsArgParse()
    bigtargets = BigTargets(args['filename'])
    bigtargets.testParallelism()
    bigtargets()

if __name__ == '__main__':
    #smalltargets()
    bigtargets()