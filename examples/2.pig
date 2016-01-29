DEFINE ArchiveLoader de.l3s.archivepig.ArchiveLoader('/data/ia/w/de');
DEFINE ExtractionStorage de.l3s.archivepig.ExtractionStorage;

cdxdata = LOAD 'data/ia/derivatives/de/cdx/*/*.cdx' USING ArchiveLoader;

cdxdata = FILTER cdxdata BY record.url MATCHES 'de,entspannungs-shop\\).*';

cdxdata = LIMIT cdxdata 5;

STORE cdxdata INTO 'results/2' USING ExtractionStorage;

