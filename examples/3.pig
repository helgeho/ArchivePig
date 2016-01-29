DEFINE ArchiveLoader de.l3s.archivepig.ArchiveLoader('/data/ia/w/de');
DEFINE ExtractionStorage de.l3s.archivepig.ExtractionStorage;

DEFINE Response de.l3s.archivepig.enrich.Response;

DEFINE Host de.l3s.archivepig.get.Host;

cdxdata = LOAD 'data/ia/derivatives/de/cdx/*/*.cdx' USING ArchiveLoader;

cdxdata = FILTER cdxdata BY Host(record) == 'entspannungs-shop.de';
--cdxdata = FILTER cdxdata BY record.url MATCHES 'de,entspannungs-shop\\).*';

cdxdata = LIMIT cdxdata 5;

response = FOREACH cdxdata GENERATE Response(response);

STORE response INTO 'results/3' USING ExtractionStorage;

