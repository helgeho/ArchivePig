DEFINE ArchiveLoader de.l3s.archivepig.ArchiveLoader('/data/ia/w/de');
DEFINE ExtractionStorage de.l3s.archivepig.ExtractionStorage;

DEFINE Response de.l3s.archivepig.enrich.Response;
DEFINE StringContent de.l3s.archivepig.enrich.StringContent;

DEFINE Host de.l3s.archivepig.get.Host;

cdxdata = LOAD 'data/ia/derivatives/de/cdx/*/*.cdx' USING ArchiveLoader;

cdxdata = FILTER cdxdata BY Host(record) == 'entspannungs-shop.de';

cdxdata = LIMIT cdxdata 5;

strings = FOREACH cdxdata GENERATE StringContent(Response(record));

STORE strings INTO 'results/4' USING ExtractionStorage;

