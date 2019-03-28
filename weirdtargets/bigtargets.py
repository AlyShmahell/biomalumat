from weirdtargets import BigTargets, BigTargetsArgParse

def main():
    args = BigTargetsArgParse()
    bigtargets = BigTargets(args['filename'], args['features_dump_path'])
    bigtargets.testParallelism()
    bigtargets()
    print(bigtargets)

if __name__ == '__main__':
    main()