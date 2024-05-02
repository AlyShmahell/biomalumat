from biomalumat.core import DiseaseTargetCycleEnumeration

if __name__ == '__main__':
    DiseaseTargetCycleEnumeration(
        "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/17.12/17.12_evidence_data.json.gz", 
        ["target", "id"],
        ["disease", "id"],
        None, "data",
    )