from weirdtargets import SmallTargets, SmallTargetsArgParse

def main():
    args = SmallTargetsArgParse()
    smalltargets = SmallTargets(args['type'], args['id'])
    smalltargets.testParallelism()
    smalltargets()
    print(smalltargets)

if __name__ == '__main__':
    main()