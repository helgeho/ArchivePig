DEFINE ArchiveLoader de.l3s.archivepig.ArchiveLoader('/data/ia/w/de');
DEFINE ExtractionStorage de.l3s.archivepig.ExtractionStorage;

cdxdata = LOAD 'data/ia/derivatives/de/cdx/TA/TA-100000-000000.arc.cdx' USING ArchiveLoader;

STORE cdxdata INTO 'results/1' USING ExtractionStorage;

