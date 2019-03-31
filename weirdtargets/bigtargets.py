from weirdtargets import BigTargets, BigTargetsArgParse

def main():
    args = BigTargetsArgParse()
    bigtargets = BigTargets(args['filename'], args['tmp_dir'])
    bigtargets.testParallelism()
    bigtargets()
    print(bigtargets)

if __name__ == '__main__':
    main()